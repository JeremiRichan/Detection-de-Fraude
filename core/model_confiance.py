# ================================================================
#  PHASE 5 — MODÈLE DE CONFIANCE
#  Score de confiance composite sur les liens entre entités
# ================================================================

import logging
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from config.configuration import (
    POIDS_FREQUENCE,
    POIDS_RECENCE,
    POIDS_DIVERSITE_PREUVES,
    POIDS_JACCARD_VOISINS,
    POIDS_CORROBORATION,
    TAUX_DECROISSANCE_RECENCE,
    FACTEUR_REDUCTION_LISTE_BLANCHE,
    NOMBRE_TYPES_PREUVES_MAX,
    ITERATIONS_MODELE_NUL,
    SEUIL_SCORE_Z_SIGNIFICATIVITE,
    SEUIL_ASYMETRIE_FORTE,
)
from core.connexion_neo4j import ConnexionNeo4j, horodatage_actuel_utc

journal = logging.getLogger("Phase5.ModelConfiance")


# ================================================================
#  COMPOSANTES DU SCORE DE CONFIANCE
# ================================================================

def _composante_frequence(nombre_interactions: int) -> float:
    """
    Score de fréquence : log-normalisé, plafonné à 1.0.
    Plus les entités interagissent fréquemment, plus le score est élevé.
    """
    if nombre_interactions <= 0:
        return 0.0
    return min(1.0, math.log1p(nombre_interactions) / math.log1p(100))


def _composante_recence(horodatage_dernier: datetime) -> float:
    """
    Score de récence : décroissance exponentielle depuis le dernier contact.
    Utilise TAUX_DECROISSANCE_RECENCE comme constante de décroissance.
    """
    maintenant = horodatage_actuel_utc()
    if horodatage_dernier.tzinfo is None:
        horodatage_dernier = horodatage_dernier.replace(tzinfo=timezone.utc)
    jours_ecoules = (maintenant - horodatage_dernier).days
    return math.exp(-TAUX_DECROISSANCE_RECENCE * jours_ecoules)


def _composante_diversite_preuves(types_preuves: List[str]) -> float:
    """
    Score de diversité : proportion des types de preuves observés
    par rapport au maximum attendu.
    """
    types_uniques = len(set(types_preuves))
    return min(1.0, types_uniques / NOMBRE_TYPES_PREUVES_MAX)


def _composante_jaccard_voisins(
    voisins_a: set,
    voisins_b: set
) -> float:
    """
    Similarité de Jaccard entre les ensembles de voisins de deux entités.
    Mesure leur appartenance commune à un réseau.
    """
    if not voisins_a and not voisins_b:
        return 0.0
    union        = voisins_a | voisins_b
    intersection = voisins_a & voisins_b
    return len(intersection) / len(union)


def _composante_corroboration(score_corroboration: float) -> float:
    """Score de corroboration inter-sources, normalisé entre 0 et 1."""
    return max(0.0, min(1.0, float(score_corroboration)))


# ================================================================
#  CALCUL DU SCORE DE CONFIANCE COMPOSITE
# ================================================================

def calculer_score_confiance(
    lien_donnees: Dict,
    connexion: ConnexionNeo4j
) -> float:
    """
    Calcule le score de confiance composite pour un lien entre entités.

    Paramètres attendus dans lien_donnees :
    - nombre_interactions (int)
    - horodatage_dernier  (datetime)
    - types_preuves       (List[str])
    - score_corroboration (float)
    - id_entite_a         (str)
    - id_entite_b         (str)
    - en_liste_blanche    (bool)

    Retourne un score entre 0.0 et 1.0.
    """
    # Récupérer les voisins depuis Neo4j
    voisins_a, voisins_b = _recuperer_voisins(
        lien_donnees.get("id_entite_a", ""),
        lien_donnees.get("id_entite_b", ""),
        connexion
    )

    score = (
        POIDS_FREQUENCE        * _composante_frequence(lien_donnees.get("nombre_interactions", 0))
        + POIDS_RECENCE        * _composante_recence(lien_donnees.get("horodatage_dernier", horodatage_actuel_utc()))
        + POIDS_DIVERSITE_PREUVES * _composante_diversite_preuves(lien_donnees.get("types_preuves", []))
        + POIDS_JACCARD_VOISINS   * _composante_jaccard_voisins(voisins_a, voisins_b)
        + POIDS_CORROBORATION     * _composante_corroboration(lien_donnees.get("score_corroboration", 0.0))
    )

    # Réduction liste blanche
    if lien_donnees.get("en_liste_blanche", False):
        score *= FACTEUR_REDUCTION_LISTE_BLANCHE

    return round(min(1.0, max(0.0, score)), 6)


def _recuperer_voisins(
    id_a: str,
    id_b: str,
    connexion: ConnexionNeo4j
) -> Tuple[set, set]:
    """Récupère les ensembles de voisins directs de deux entités."""
    requete = """
        MATCH (e:Entite {identifiant: $identifiant})-[]-(voisin:Entite)
        RETURN collect(DISTINCT voisin.identifiant) AS voisins
    """
    res_a = connexion.executer_requete_lecture(requete, {"identifiant": id_a})
    res_b = connexion.executer_requete_lecture(requete, {"identifiant": id_b})
    voisins_a = set(res_a[0]["voisins"]) if res_a else set()
    voisins_b = set(res_b[0]["voisins"]) if res_b else set()
    return voisins_a, voisins_b


# ================================================================
#  MODÈLE NUL ET SIGNIFICATIVITÉ STATISTIQUE
# ================================================================

def evaluer_significativite(
    score_observe: float,
    lien_donnees: Dict,
    connexion: ConnexionNeo4j
) -> Dict:
    """
    Compare le score observé à la distribution nulle (Monte Carlo).
    Retourne le score Z et un indicateur de significativité.
    """
    scores_nuls = []
    for _ in range(ITERATIONS_MODELE_NUL):
        lien_aleatoire = {
            "nombre_interactions": random.randint(1, 50),
            "horodatage_dernier":  horodatage_actuel_utc() - timedelta(days=random.randint(0, 365)),
            "types_preuves":       random.choices(["EMAIL", "TRANSACTION", "SMS", "LOG_ACCES"], k=random.randint(1, 5)),
            "score_corroboration": random.random(),
            "id_entite_a":         "",
            "id_entite_b":         "",
            "en_liste_blanche":    False,
        }
        # Score simplifié sans appel Neo4j pour le modèle nul
        score_nul = (
            POIDS_FREQUENCE    * _composante_frequence(lien_aleatoire["nombre_interactions"])
            + POIDS_RECENCE    * _composante_recence(lien_aleatoire["horodatage_dernier"])
            + POIDS_DIVERSITE_PREUVES * _composante_diversite_preuves(lien_aleatoire["types_preuves"])
            + POIDS_CORROBORATION     * _composante_corroboration(lien_aleatoire["score_corroboration"])
        )
        scores_nuls.append(score_nul)

    moyenne_nulle = sum(scores_nuls) / len(scores_nuls)
    variance_nulle = sum((s - moyenne_nulle) ** 2 for s in scores_nuls) / len(scores_nuls)
    ecart_type_nul = math.sqrt(variance_nulle) if variance_nulle > 0 else 1e-9

    score_z = (score_observe - moyenne_nulle) / ecart_type_nul
    significatif = abs(score_z) >= SEUIL_SCORE_Z_SIGNIFICATIVITE

    # Asymétrie (skewness)
    asymetrie = (
        sum((s - moyenne_nulle) ** 3 for s in scores_nuls)
        / (len(scores_nuls) * ecart_type_nul ** 3)
    ) if ecart_type_nul > 0 else 0.0

    return {
        "score_z":        round(score_z, 4),
        "significatif":   significatif,
        "asymetrie_forte": abs(asymetrie) > SEUIL_ASYMETRIE_FORTE,
        "moyenne_nulle":  round(moyenne_nulle, 6),
        "ecart_type_nul": round(ecart_type_nul, 6),
    }


# ================================================================
#  PIPELINE MODÈLE DE CONFIANCE
# ================================================================

def calculer_confiances_liens(connexion: ConnexionNeo4j) -> int:
    """
    Calcule et persiste le score de confiance pour tous les liens
    du graphe qui n'en ont pas encore.
    Retourne le nombre de liens traités.
    """
    requete_liens = """
        MATCH (a:Entite)-[lien:CAUSE_DE|PARTAGE_IP|COSIGNATURE]->(b:Entite)
        WHERE lien.score_confiance IS NULL
        RETURN
            a.identifiant AS id_a,
            b.identifiant AS id_b,
            type(lien)    AS type_lien,
            lien.delta_secondes AS delta,
            id(lien)      AS id_lien
        LIMIT 1000
    """
    liens = connexion.executer_requete_lecture(requete_liens)
    traites = 0

    for lien in liens:
        donnees = {
            "id_entite_a":         lien["id_a"],
            "id_entite_b":         lien["id_b"],
            "nombre_interactions": 1,
            "horodatage_dernier":  horodatage_actuel_utc(),
            "types_preuves":       [lien["type_lien"]],
            "score_corroboration": 0.5,
            "en_liste_blanche":    False,
        }
        score = calculer_score_confiance(donnees, connexion)

        requete_maj = """
            MATCH ()-[lien]->()
            WHERE id(lien) = $id_lien
            SET lien.score_confiance = $score, lien.date_confiance = datetime()
        """
        connexion.executer_requete_ecriture(requete_maj, {
            "id_lien": lien["id_lien"],
            "score":   score,
        })
        traites += 1

    journal.info(f"Scores de confiance calculés : {traites} liens")
    return traites