import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from config.configuration import (
    DUREE_FENETRE_CAUSALITE_HEURES,
    DUREE_INACTIVITE_LIEN_JOURS,
    PATRONS_CAUSAUX_FRAUDE,
)
from core.connexion_neo4j import (
    ConnexionNeo4j,
    convertir_horodatage_utc,
    horodatage_actuel_utc,
)

journal = logging.getLogger("Phase3.ConstructionGraphe")



#  CRÉATION DES NŒUDS ÉVÉNEMENTS


def creer_noeud_evenement(
    evenement: Dict,
    connexion: ConnexionNeo4j
) -> str:
    """
    Crée ou met à jour un nœud Evenement dans Neo4j.
    Retourne l'identifiant UUID du nœud créé.
    Utilise MERGE pour éviter les doublons résiduels.
    """
    requete = """
        MERGE (e:Evenement {identifiant_source: $identifiant_source})
        ON CREATE SET
            e.identifiant       = randomUUID(),
            e.type_evenement    = $type_evenement,
            e.horodatage        = datetime($horodatage),
            e.montant           = $montant,
            e.id_entite_source  = $id_entite_source,
            e.id_entite_cible   = $id_entite_cible,
            e.flags_semantiques = $flags_semantiques,
            e.date_creation     = datetime()
        ON MATCH SET
            e.flags_semantiques = $flags_semantiques,
            e.date_mise_a_jour  = datetime()
        RETURN e.identifiant AS identifiant
    """
    horodatage = evenement.get("horodatage")
    if isinstance(horodatage, datetime):
        horodatage_iso = horodatage.isoformat()
    else:
        horodatage_iso = str(horodatage)

    parametres = {
        "identifiant_source": evenement.get("id_source", ""),
        "type_evenement":     evenement.get("type_evenement", ""),
        "horodatage":         horodatage_iso,
        "montant":            float(evenement.get("montant", 0.0)),
        "id_entite_source":   evenement.get("id_entite_source", ""),
        "id_entite_cible":    evenement.get("id_entite_cible", ""),
        "flags_semantiques":  evenement.get("flags_semantiques", []),
    }
    resultats = connexion.executer_requete_ecriture(requete, parametres)
    return resultats[0]["identifiant"] if resultats else ""


#  CRÉATION DES LIENS CAUSAUX


def _est_patron_causal(type_a: str, type_b: str) -> bool:
    """Vérifie si le couple (type_a, type_b) est un patron causal de fraude."""
    return (type_a, type_b) in PATRONS_CAUSAUX_FRAUDE


def creer_lien_causal(
    id_evenement_cause: str,
    id_evenement_effet: str,
    delta_secondes: float,
    connexion: ConnexionNeo4j
) -> None:
    """
    Crée un lien CAUSE_DE entre deux événements dans la fenêtre causale.
    """
    requete = """
        MATCH (cause:Evenement {identifiant: $id_cause})
        MATCH (effet:Evenement  {identifiant: $id_effet})
        MERGE (cause)-[lien:CAUSE_DE]->(effet)
        ON CREATE SET
            lien.delta_secondes = $delta_secondes,
            lien.date_creation  = datetime()
    """
    connexion.executer_requete_ecriture(requete, {
        "id_cause":       id_evenement_cause,
        "id_effet":       id_evenement_effet,
        "delta_secondes": delta_secondes,
    })


def construire_liens_causaux(
    evenements: List[Dict],
    connexion: ConnexionNeo4j
) -> int:
    """
    Analyse tous les couples d'événements et crée les liens causaux
    pour les couples dont le type correspond à un patron de fraude
    et dont l'écart temporel est dans la fenêtre de causalité.

    Retourne le nombre de liens créés.
    """
    fenetre = timedelta(hours=DUREE_FENETRE_CAUSALITE_HEURES)
    liens_crees = 0

    # Trier par horodatage
    tries = sorted(
        evenements,
        key=lambda e: convertir_horodatage_utc(e["horodatage"]) if e.get("horodatage") else datetime.min.replace(tzinfo=timezone.utc)
    )

    for i, evt_cause in enumerate(tries):
        horodatage_cause = convertir_horodatage_utc(evt_cause["horodatage"])
        type_cause = evt_cause.get("type_evenement", "")

        for evt_effet in tries[i + 1:]:
            horodatage_effet = convertir_horodatage_utc(evt_effet["horodatage"])
            delta = horodatage_effet - horodatage_cause

            if delta > fenetre:
                break  # Liste triée : inutile de continuer

            type_effet = evt_effet.get("type_evenement", "")
            if not _est_patron_causal(type_cause, type_effet):
                continue

            # Même entité source ou cible impliquée
            source_commune = (
                evt_cause.get("id_entite_source") == evt_effet.get("id_entite_source")
                or evt_cause.get("id_entite_cible") == evt_effet.get("id_entite_source")
            )
            if not source_commune:
                continue

            creer_lien_causal(
                evt_cause.get("_neo4j_id", evt_cause.get("id_source", "")),
                evt_effet.get("_neo4j_id", evt_effet.get("id_source", "")),
                delta.total_seconds(),
                connexion
            )
            liens_crees += 1

    journal.info(f"Liens causaux créés : {liens_crees}")
    return liens_crees



#  GESTION DE L'INACTIVITÉ DES LIENS

def marquer_liens_inactifs(connexion: ConnexionNeo4j) -> int:
    """
    Marque comme inactifs les liens n'ayant pas été mis à jour
    depuis DUREE_INACTIVITE_LIEN_JOURS jours.
    Retourne le nombre de liens marqués.
    """
    requete = """
        MATCH ()-[lien:CAUSE_DE]->()
        WHERE lien.date_creation < datetime() - duration({days: $jours_inactivite})
          AND lien.inactif IS NULL
        SET lien.inactif = true,
            lien.date_inactivation = datetime()
        RETURN count(lien) AS nombre_inactifs
    """

    resultats = connexion.executer_requete_ecriture(
        requete,
        {"jours_inactivite": DUREE_INACTIVITE_LIEN_JOURS}
    )

    nombre = resultats[0]["nombre_inactifs"] if resultats else 0

    journal.info(f"Liens inactifs marqués : {nombre}")

    return nombre

#  PIPELINE CONSTRUCTION GRAPHE


def construire_graphe(
    evenements_valides: List[Dict],
    connexion: ConnexionNeo4j
) -> Dict:
    """
    Pipeline complet de construction du graphe :
    1. Création des nœuds événements
    2. Construction des liens causaux
    3. Nettoyage des liens inactifs

    Retourne un résumé des opérations.
    """
    journal.info(f"Construction du graphe pour {len(evenements_valides)} événements")

    # Étape 1 — Nœuds
    ids_crees = []
    for evt in evenements_valides:
        identifiant = creer_noeud_evenement(evt, connexion)
        evt["_neo4j_id"] = identifiant
        ids_crees.append(identifiant)

    # Étape 2 — Liens causaux
    nombre_liens = construire_liens_causaux(evenements_valides, connexion)

    # Étape 3 — Inactivité
    nombre_inactifs = marquer_liens_inactifs(connexion)

    resume = {
        "noeuds_crees":    len(ids_crees),
        "liens_causaux":   nombre_liens,
        "liens_inactifs":  nombre_inactifs,
    }
    journal.info(f"Graphe construit : {resume}")
    return resume