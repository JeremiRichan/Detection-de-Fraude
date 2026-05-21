# ================================================================
#  PHASE 7 — SCORING ET DÉCISION
#  Isolation Forest + GNN simulé, seuil de coût, vecteur de features
# ================================================================

import logging
import math
import random
from typing import Dict, List, Optional, Tuple

from config.configuration import (
    NOMBRE_ARBRES_ISOLATION_FOREST,
    TAILLE_ECHANTILLON_ISOLATION,
    POIDS_ISOLATION_FOREST,
    POIDS_GNN,
    COUT_FAUX_NEGATIF,
    COUT_FAUX_POSITIF,
    DIMENSION_VECTEUR_CARACTERISTIQUES,
)
from core.connexion_neo4j import ConnexionNeo4j, horodatage_actuel_utc

journal = logging.getLogger("Phase7.ScoringDecision")


# ================================================================
#  EXTRACTION DU VECTEUR DE CARACTÉRISTIQUES
# ================================================================

def extraire_vecteur_caracteristiques(
    entite_id: str,
    connexion: ConnexionNeo4j
) -> List[float]:
    """
    Extrait le vecteur de caractéristiques d'une entité depuis Neo4j.
    Dimensions (DIMENSION_VECTEUR_CARACTERISTIQUES = 16) :
      0  — PageRank
      1  — Degré entrant
      2  — Degré sortant
      3  — Score de confiance moyen des liens
      4  — Cohésion de la communauté d'appartenance
      5  — Score d'identité synthétique (0/1)
      6  — Nombre de flags sémantiques
      7  — Montant moyen des transactions
      8  — Écart-type des montants
      9  — Nombre de communautés visitées
      10 — Ancienneté en jours
      11 — Nombre de fusions subies
      12 — Score PageRank de la communauté
      13 — Ratio liens inactifs / total
      14 — Nombre de hubs dans le voisinage
      15 — Score de corroboration moyen
    """
    requete = """
        MATCH (e:Entite {identifiant: $id})
        OPTIONAL MATCH (e)-[lien_sortant]->()
        OPTIONAL MATCH ()-[lien_entrant]->(e)
        OPTIONAL MATCH (e)-[l:CAUSE_DE|PARTAGE_IP]->(voisin)
        WITH e,
             count(DISTINCT lien_sortant) AS degre_sortant,
             count(DISTINCT lien_entrant) AS degre_entrant,
             avg(coalesce(l.score_confiance, 0.0)) AS confiance_moy,
             count(CASE WHEN l.inactif = true THEN 1 END) AS liens_inactifs,
             count(l) AS liens_total
        RETURN
            coalesce(e.score_pagerank, 0.0)        AS pagerank,
            degre_entrant,
            degre_sortant,
            confiance_moy,
            coalesce(e.cohesion_communaute, 0.0)   AS cohesion,
            CASE WHEN e.identite_synthetique THEN 1.0 ELSE 0.0 END AS synthetique,
            coalesce(size(e.flags_semantiques), 0)  AS nb_flags,
            coalesce(e.montant_moyen, 0.0)          AS montant_moyen,
            coalesce(e.montant_ecart_type, 0.0)     AS montant_std,
            coalesce(e.nb_communautes, 1)           AS nb_communautes,
            coalesce(duration.between(e.date_creation, datetime()).days, 0) AS anciennete,
            coalesce(size(e.identifiants_fusionnes), 0) AS nb_fusions,
            coalesce(e.pagerank_communaute, 0.0)    AS pagerank_com,
            CASE WHEN liens_total > 0
                 THEN toFloat(liens_inactifs) / liens_total
                 ELSE 0.0 END                        AS ratio_inactifs,
            coalesce(e.nb_hubs_voisins, 0)          AS nb_hubs,
            coalesce(e.score_corroboration_moyen, 0.5) AS corroboration
    """
    try:
        resultats = connexion.executer_requete_lecture(requete, {"id": entite_id})
    except Exception as err:
        journal.error(f"Impossible d'extraire les features pour '{entite_id}' : {err}")
        return [0.0] * DIMENSION_VECTEUR_CARACTERISTIQUES

    if not resultats:
        return [0.0] * DIMENSION_VECTEUR_CARACTERISTIQUES

    r = resultats[0]
    vecteur = [
        float(r.get("pagerank",       0.0)),
        float(r.get("degre_entrant",  0)),
        float(r.get("degre_sortant",  0)),
        float(r.get("confiance_moy",  0.0)),
        float(r.get("cohesion",       0.0)),
        float(r.get("synthetique",    0.0)),
        float(r.get("nb_flags",       0)),
        float(r.get("montant_moyen",  0.0)),
        float(r.get("montant_std",    0.0)),
        float(r.get("nb_communautes", 1)),
        float(r.get("anciennete",     0)),
        float(r.get("nb_fusions",     0)),
        float(r.get("pagerank_com",   0.0)),
        float(r.get("ratio_inactifs", 0.0)),
        float(r.get("nb_hubs",        0)),
        float(r.get("corroboration",  0.5)),
    ]
    return vecteur


# ================================================================
#  ISOLATION FOREST SIMPLIFIÉ
# ================================================================

class ArbreIsolation:
    """Arbre d'isolation binaire pour détection d'anomalies."""

    def __init__(self, profondeur_max: int):
        self.profondeur_max = profondeur_max
        self.dimension: Optional[int] = None
        self.seuil: Optional[float] = None
        self.gauche: Optional["ArbreIsolation"] = None
        self.droite: Optional["ArbreIsolation"] = None

    def construire(self, donnees: List[List[float]], profondeur: int = 0) -> None:
        n = len(donnees)
        if n <= 1 or profondeur >= self.profondeur_max:
            return
        dim = len(donnees[0])
        self.dimension = random.randint(0, dim - 1)
        valeurs = [d[self.dimension] for d in donnees]
        v_min, v_max = min(valeurs), max(valeurs)
        if v_min == v_max:
            return
        self.seuil = random.uniform(v_min, v_max)
        gauche_data = [d for d in donnees if d[self.dimension] < self.seuil]
        droite_data = [d for d in donnees if d[self.dimension] >= self.seuil]
        if gauche_data:
            self.gauche = ArbreIsolation(self.profondeur_max)
            self.gauche.construire(gauche_data, profondeur + 1)
        if droite_data:
            self.droite = ArbreIsolation(self.profondeur_max)
            self.droite.construire(droite_data, profondeur + 1)

    def profondeur_point(self, point: List[float], prof: int = 0) -> int:
        if self.dimension is None or self.seuil is None:
            return prof
        if point[self.dimension] < self.seuil:
            return self.gauche.profondeur_point(point, prof + 1) if self.gauche else prof + 1
        return self.droite.profondeur_point(point, prof + 1) if self.droite else prof + 1


def _c(n: int) -> float:
    """Facteur de normalisation de l'Isolation Forest."""
    if n <= 1:
        return 0.0
    return 2 * (math.log(n - 1) + 0.5772156649) - 2 * (n - 1) / n


def scorer_isolation_forest(
    vecteur: List[float],
    donnees_entrainement: List[List[float]]
) -> float:
    """
    Calcule le score d'anomalie Isolation Forest pour un vecteur.
    Retourne un score entre 0.0 (normal) et 1.0 (anomalie).
    """
    n = len(donnees_entrainement)
    if n < 2:
        return 0.5

    profondeur_max = math.ceil(math.log2(min(TAILLE_ECHANTILLON_ISOLATION, n)))
    profondeurs_moyennes = []

    for _ in range(NOMBRE_ARBRES_ISOLATION_FOREST):
        echantillon = random.sample(
            donnees_entrainement,
            min(TAILLE_ECHANTILLON_ISOLATION, n)
        )
        arbre = ArbreIsolation(profondeur_max)
        arbre.construire(echantillon)
        profondeurs_moyennes.append(arbre.profondeur_point(vecteur))

    profondeur_moy = sum(profondeurs_moyennes) / len(profondeurs_moyennes)
    c_n = _c(min(TAILLE_ECHANTILLON_ISOLATION, n))
    if c_n == 0:
        return 0.5
    score = 2 ** (-profondeur_moy / c_n)
    return round(min(1.0, max(0.0, score)), 6)


# ================================================================
#  GNN SIMULÉ (propagation de risque sur le graphe)
# ================================================================

def scorer_gnn(
    entite_id: str,
    connexion: ConnexionNeo4j,
    nb_couches: int = 2
) -> float:
    """
    Simulation de GNN par propagation de messages sur le voisinage.
    Agrège les scores de risque des voisins (moyenne pondérée).
    Retourne un score entre 0.0 et 1.0.
    """
    requete = """
        MATCH (e:Entite {identifiant: $id})
        OPTIONAL MATCH (e)-[l]->(v:Entite)
        RETURN
            coalesce(e.score_risque_precedent, 0.1) AS score_propre,
            collect({
                score: coalesce(v.score_risque_precedent, 0.1),
                poids: coalesce(l.score_confiance, 0.5)
            }) AS voisins
    """
    resultats = connexion.executer_requete_lecture(requete, {"id": entite_id})
    if not resultats:
        return 0.1

    r = resultats[0]
    score_propre = float(r.get("score_propre", 0.1))
    voisins      = r.get("voisins", []) or []

    if not voisins:
        return score_propre

    score_agrege = score_propre
    for _ in range(nb_couches):
        numerateur   = sum(float(v["score"]) * float(v["poids"]) for v in voisins)
        denominateur = sum(float(v["poids"]) for v in voisins)
        if denominateur > 0:
            score_agrege = 0.5 * score_agrege + 0.5 * (numerateur / denominateur)

    return round(min(1.0, max(0.0, score_agrege)), 6)


# ================================================================
#  CALCUL DU SEUIL DE DÉCISION PAR COÛT
# ================================================================

def calculer_seuil_optimal() -> float:
    """
    Calcule le seuil de décision optimal selon les coûts métier.
    Seuil = C_FP / (C_FP + C_FN).
    """
    seuil = COUT_FAUX_POSITIF / (COUT_FAUX_POSITIF + COUT_FAUX_NEGATIF)
    journal.info(
        f"Seuil optimal = {seuil:.4f} "
        f"(C_FN={COUT_FAUX_NEGATIF:.0f}, C_FP={COUT_FAUX_POSITIF:.0f})"
    )
    return seuil


# ================================================================
#  SCORE COMPOSITE ET DÉCISION FINALE
# ================================================================

def scorer_entite(
    entite_id: str,
    connexion: ConnexionNeo4j,
    donnees_entrainement: Optional[List[List[float]]] = None
) -> Dict:
    """
    Calcule le score de risque composite pour une entité.
    Combine Isolation Forest et GNN selon les poids configurés.
    Retourne le score, la décision et les détails.
    """
    vecteur = extraire_vecteur_caracteristiques(entite_id, connexion)

    # Isolation Forest
    if donnees_entrainement and len(donnees_entrainement) >= 2:
        score_if = scorer_isolation_forest(vecteur, donnees_entrainement)
    else:
        # Heuristique si pas de données d'entraînement
        score_if = min(1.0, sum(vecteur[5:8]) / 3.0)  # flags + synthétique + anomalie montant

    # GNN
    score_gnn = scorer_gnn(entite_id, connexion)

    # Score composite
    score_final = POIDS_ISOLATION_FOREST * score_if + POIDS_GNN * score_gnn
    seuil       = calculer_seuil_optimal()
    decision    = "FRAUDE_SUSPECTEE" if score_final >= seuil else "NORMAL"

    # Persister le score
    requete_maj = """
        MATCH (e:Entite {identifiant: $id})
        SET e.score_risque          = $score,
            e.score_risque_precedent = e.score_risque,
            e.decision_fraude       = $decision,
            e.date_scoring          = datetime()
    """
    connexion.executer_requete_ecriture(requete_maj, {
        "id":       entite_id,
        "score":    score_final,
        "decision": decision,
    })

    resultat = {
        "identifiant":        entite_id,
        "score_final":        round(score_final, 6),
        "score_isolation_forest": round(score_if, 6),
        "score_gnn":          round(score_gnn, 6),
        "seuil":              round(seuil, 6),
        "decision":           decision,
    }
    journal.info(f"Score '{entite_id}' : {score_final:.4f} → {decision}")
    return resultat


def scorer_lot(
    entites_ids: List[str],
    connexion: ConnexionNeo4j,
    donnees_entrainement: Optional[List[List[float]]] = None
) -> List[Dict]:
    """Calcule les scores pour un lot d'entités."""
    resultats = []
    for eid in entites_ids:
        resultats.append(scorer_entite(eid, connexion, donnees_entrainement))
    suspects = sum(1 for r in resultats if r["decision"] == "FRAUDE_SUSPECTEE")
    journal.info(f"Scoring lot : {len(resultats)} entités, {suspects} suspects")
    return resultats