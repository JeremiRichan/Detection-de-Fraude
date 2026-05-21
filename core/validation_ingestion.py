# ================================================================
#  PHASE 1 — VALIDATION ET INGESTION DES ÉVÉNEMENTS
#  Algorithmes V1 à V8 : filtrage, déduplication, corroboration
#
#  OPTIMISATIONS :
#  - Validations V1-V4, V6-V8 : 100% en mémoire, zéro appel Neo4j
#  - Rejets regroupés en un seul batch UNWIND (une requête Neo4j)
#  - V5 rétrodatation : fenêtre portée à JOURS_HISTORIQUE du générateur
#  - Logs WARNING supprimés pour les cas normaux de données historiques
# ================================================================

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import json

from config.configuration import (
    TYPES_EVENEMENTS_AUTORISES,
    TYPES_SOURCE_AUTORISES,
    DUREE_FENETRE_DEDUPLICATION_SEC,
    FACTEUR_SPIKE_VOLUME,
    DUREE_FENETRE_SPIKE_MINUTES,
    DUREE_RETRODATE_MAXIMUM_JOURS,
    SEUIL_CORROBORATION_MINIMUM,
    NOMBRE_SOURCES_MAXIMUM,
    FACTEUR_ABERRANCE_MONTANT,
    SAUT_HIERARCHIQUE_MAXIMUM,
)
from core.connexion_neo4j import (
    ConnexionNeo4j,
    convertir_horodatage_utc,
    horodatage_actuel_utc,
    marquer_evenement,
)

journal = logging.getLogger("Phase1.Ingestion")

# Réduire le bruit des notifications Neo4j (cartesian product, labels manquants)
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)


# ================================================================
#  VALIDATEURS EN MÉMOIRE (V1–V8, sans appel Neo4j)
# ================================================================

# Caches en mémoire
_cache_deduplication: Dict[str, datetime] = {}
_historique_volumes:  Dict[str, List[datetime]] = defaultdict(list)
_historique_montants: Dict[str, List[float]] = defaultdict(list)


def v1_filtrer_type_evenement(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    type_evt = evt.get("type_evenement", "")
    if type_evt not in TYPES_EVENEMENTS_AUTORISES:
        return None, f"Type non autorisé : '{type_evt}'"
    return evt, None


def v2_valider_source(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    source = evt.get("type_source", "")
    if source not in TYPES_SOURCE_AUTORISES:
        return None, f"Source non autorisée : '{source}'"
    return evt, None


def v3_dedupliquer(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    cle = f"{evt.get('id_source')}|{evt.get('type_evenement')}|{evt.get('montant', '')}"
    maintenant = horodatage_actuel_utc()
    fenetre    = timedelta(seconds=DUREE_FENETRE_DEDUPLICATION_SEC)
    if cle in _cache_deduplication:
        if maintenant - _cache_deduplication[cle] < fenetre:
            return None, f"Doublon (fenêtre={DUREE_FENETRE_DEDUPLICATION_SEC}s)"
    _cache_deduplication[cle] = maintenant
    return evt, None


def v4_detecter_spike_volume(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    id_src     = evt.get("id_source", "INCONNU")
    maintenant = horodatage_actuel_utc()
    fenetre    = timedelta(minutes=DUREE_FENETRE_SPIKE_MINUTES)
    _historique_volumes[id_src] = [
        ts for ts in _historique_volumes[id_src] if maintenant - ts < fenetre
    ]
    _historique_volumes[id_src].append(maintenant)
    volume = len(_historique_volumes[id_src])
    moyen  = max(1, volume / max(1, DUREE_FENETRE_SPIKE_MINUTES))
    if volume > FACTEUR_SPIKE_VOLUME * moyen:
        evt = marquer_evenement(evt, "SPIKE_VOLUME")
    return evt, None


def v5_verifier_retrodate(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    horodatage_brut = evt.get("horodatage")
    if horodatage_brut is None:
        return None, "Horodatage manquant"
    try:
        horodatage = convertir_horodatage_utc(horodatage_brut)
    except ValueError as e:
        return None, f"Horodatage invalide : {e}"

    maintenant    = horodatage_actuel_utc()
    age           = (maintenant - horodatage).days
    # Fenêtre élargie : on accepte jusqu'à 180 jours (cohérent avec le générateur)
    # Les événements > 30 jours sont marqués RETRODATE_MODEREE mais pas rejetés
    if age > 180:
        return None, f"Rétrodatation excessive : {age} jours > 180 jours"
    if age > DUREE_RETRODATE_MAXIMUM_JOURS:
        evt = marquer_evenement(evt, "RETRODATE_MODEREE")
    return evt, None


def v6_controler_corroboration(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    score  = float(evt.get("score_corroboration", 0.0))
    nb_src = int(evt.get("nombre_sources", 1))
    if nb_src > NOMBRE_SOURCES_MAXIMUM:
        return None, f"Nombre de sources excessif : {nb_src}"
    if score < SEUIL_CORROBORATION_MINIMUM:
        evt = marquer_evenement(evt, "FAIBLE_CORROBORATION")
    return evt, None


def v7_detecter_aberrance_montant(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    type_evt = evt.get("type_evenement")
    montant  = evt.get("montant")
    if montant is None or type_evt != "TRANSACTION":
        return evt, None
    montant = float(montant)
    historique = _historique_montants[type_evt]
    if len(historique) >= 10:
        mediane = sorted(historique)[len(historique) // 2]
        if mediane > 0 and montant > FACTEUR_ABERRANCE_MONTANT * mediane:
            evt = marquer_evenement(evt, "MONTANT_ABERRANT")
    historique.append(montant)
    if len(historique) > 1000:
        _historique_montants[type_evt] = historique[-1000:]
    return evt, None


def v8_controler_saut_hierarchique(evt: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    saut = int(evt.get("saut_hierarchique", 0))
    if saut > SAUT_HIERARCHIQUE_MAXIMUM:
        return None, f"Saut hiérarchique excessif : {saut}"
    return evt, None


PIPELINE_VALIDATION = [
    v1_filtrer_type_evenement,
    v2_valider_source,
    v3_dedupliquer,
    v4_detecter_spike_volume,
    v5_verifier_retrodate,
    v6_controler_corroboration,
    v7_detecter_aberrance_montant,
    v8_controler_saut_hierarchique,
]


# ================================================================
#  ENREGISTREMENT BATCH DES REJETS (1 seule requête Neo4j)
# ================================================================

def _enregistrer_rejets_batch(
    rejets: List[Dict],
    connexion: ConnexionNeo4j
) -> None:
    """
    Enregistre tous les rejets en une seule requête UNWIND.
    Remplace N appels Neo4j par 1 seul — gain de performance majeur.
    """
    if not rejets:
        return
    requete = """
        UNWIND $rejets AS r
        CREATE (n:EvenementRejete {
            identifiant_rejet:  randomUUID(),
            identifiant_source: r.identifiant_source,
            type_evenement:     r.type_evenement,
            motif_rejet:        r.motif_rejet,
            horodatage_rejet:   datetime(),
            donnees_brutes:     r.donnees_brutes
        })
    """
    donnees = [
        {
            "identifiant_source": r.get("id_source", "INCONNU"),
            "type_evenement":     r.get("type_evenement", "INCONNU"),
            "motif_rejet":        r["_motif"],
            "donnees_brutes":     json.dumps(
                {k: v for k, v in r.items() if k != "_motif"},
                default=str, ensure_ascii=False
            )[:500],
        }
        for r in rejets
    ]
    connexion.executer_requete_ecriture(requete, {"rejets": donnees})


# ================================================================
#  PIPELINE PRINCIPAL — TRAITEMENT 100% EN MÉMOIRE
# ================================================================

def ingerer_lot(
    evenements: List[Dict],
    connexion: ConnexionNeo4j
) -> Tuple[List[Dict], int]:
    """
    Valide un lot d'événements entièrement en mémoire (V1–V8),
    puis enregistre tous les rejets en une seule requête batch.

    Performance : O(n) en mémoire + 1 requête Neo4j pour les rejets.
    Retourne (événements_valides, nombre_rejets).
    """
    valides: List[Dict] = []
    rejets:  List[Dict] = []

    # Compteurs par motif pour le résumé final
    compteurs_rejets: Dict[str, int] = defaultdict(int)

    for evt_brut in evenements:
        evt = evt_brut.copy()
        rejete = False

        for validateur in PIPELINE_VALIDATION:
            evt, motif = validateur(evt)
            if motif is not None:
                evt_brut["_motif"] = motif
                rejets.append(evt_brut)
                compteurs_rejets[motif.split(":")[0].strip()] += 1
                rejete = True
                break

        if not rejete:
            valides.append(evt)

    # Un seul appel Neo4j pour tous les rejets
    _enregistrer_rejets_batch(rejets, connexion)

    # Résumé compact
    journal.info(
        f"Ingestion : {len(valides)} acceptés | {len(rejets)} rejetés "
        f"({', '.join(f'{k}={v}' for k, v in compteurs_rejets.items())})"
    )
    return valides, len(rejets)