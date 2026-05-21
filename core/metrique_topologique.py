# ================================================================
#  PHASE 4 — MÉTRIQUES TOPOLOGIQUES
#  PageRank, détection de hubs, cliques, centralité
# ================================================================

import logging
import math
from typing import Dict, List, Optional, Tuple

from config.configuration import (
    FACTEUR_AMORTISSEMENT_PAGERANK,
    ITERATIONS_MAX_PAGERANK,
    TOLERANCE_CONVERGENCE_PAGERANK,
    TAILLE_CLIQUE_MINIMUM,
    SEUIL_MULTIPLICATEUR_HUB,
)
from core.connexion_neo4j import ConnexionNeo4j

journal = logging.getLogger("Phase4.MetriqueTopologique")


# ================================================================
#  PAGERANK PERSONNALISÉ
# ================================================================

def calculer_pagerank(connexion: ConnexionNeo4j) -> Dict[str, float]:
    """
    Calcule le PageRank de chaque entité dans le graphe Neo4j.
    Utilise l'algorithme itératif avec facteur d'amortissement.
    Retourne un dictionnaire {identifiant_entite: score_pagerank}.
    """
    # Récupérer tous les nœuds et leurs voisins sortants
    requete_graphe = """
        MATCH (source:Entite)-[:IMPLIQUE|CAUSE_DE*1..2]->(cible:Entite)
        RETURN source.identifiant AS source, cible.identifiant AS cible
    """
    aretes = connexion.executer_requete_lecture(requete_graphe)

    # Construire la liste d'adjacence
    successeurs: Dict[str, List[str]] = {}
    noeuds: set = set()
    for arete in aretes:
        s, c = arete["source"], arete["cible"]
        noeuds.add(s)
        noeuds.add(c)
        successeurs.setdefault(s, []).append(c)

    if not noeuds:
        journal.warning("Aucun nœud trouvé pour PageRank")
        return {}

    n = len(noeuds)
    scores = {noeud: 1.0 / n for noeud in noeuds}
    d = FACTEUR_AMORTISSEMENT_PAGERANK

    for iteration in range(ITERATIONS_MAX_PAGERANK):
        nouveaux_scores: Dict[str, float] = {}
        for noeud in noeuds:
            contribution = 0.0
            for autre in noeuds:
                voisins = successeurs.get(autre, [])
                if noeud in voisins and len(voisins) > 0:
                    contribution += scores[autre] / len(voisins)
            nouveaux_scores[noeud] = (1 - d) / n + d * contribution

        # Vérifier la convergence
        delta_max = max(
            abs(nouveaux_scores[n_] - scores[n_]) for n_ in noeuds
        )
        scores = nouveaux_scores

        if delta_max < TOLERANCE_CONVERGENCE_PAGERANK:
            journal.info(f"PageRank convergé en {iteration + 1} itérations (delta={delta_max:.2e})")
            break
    else:
        journal.warning(f"PageRank non convergé après {ITERATIONS_MAX_PAGERANK} itérations")

    # Persister les scores dans Neo4j
    _persister_pagerank(scores, connexion)
    return scores


def _persister_pagerank(
    scores: Dict[str, float],
    connexion: ConnexionNeo4j
) -> None:
    """Met à jour le score PageRank de chaque entité dans Neo4j."""
    requete = """
        MATCH (e:Entite {identifiant: $identifiant})
        SET e.score_pagerank = $score, e.date_pagerank = datetime()
    """
    requetes = [
        (requete, {"identifiant": ident, "score": score})
        for ident, score in scores.items()
    ]
    connexion.executer_transaction_multiple(requetes)


# ================================================================
#  DÉTECTION DE HUBS
# ================================================================

def detecter_hubs(connexion: ConnexionNeo4j) -> List[Dict]:
    """
    Identifie les nœuds dont le degré dépasse SEUIL_MULTIPLICATEUR_HUB
    fois le degré moyen du graphe.
    Ces nœuds sont des coordinateurs potentiels de fraude.
    """
    requete_degres = """
        MATCH (e:Entite)
        OPTIONAL MATCH (e)-[r]-()
        WITH e.identifiant AS identifiant, count(r) AS degre
        RETURN identifiant, degre
        ORDER BY degre DESC
    """
    resultats = connexion.executer_requete_lecture(requete_degres)
    if not resultats:
        return []

    degres     = [r["degre"] for r in resultats]
    degre_moyen = sum(degres) / len(degres) if degres else 0
    seuil_hub  = SEUIL_MULTIPLICATEUR_HUB * degre_moyen

    hubs = [
        {"identifiant": r["identifiant"], "degre": r["degre"]}
        for r in resultats
        if r["degre"] >= seuil_hub
    ]

    # Marquer les hubs dans Neo4j
    if hubs:
        requete_marquage = """
            MATCH (e:Entite {identifiant: $identifiant})
            SET e.est_hub = true, e.degre = $degre
        """
        connexion.executer_transaction_multiple([
            (requete_marquage, {"identifiant": h["identifiant"], "degre": h["degre"]})
            for h in hubs
        ])

    journal.info(f"Hubs détectés : {len(hubs)} (seuil={seuil_hub:.1f}, moyenne={degre_moyen:.1f})")
    return hubs


# ================================================================
#  DÉTECTION DE CLIQUES
# ================================================================

def detecter_cliques(connexion: ConnexionNeo4j) -> List[List[str]]:
    """
    Détecte les cliques de taille >= TAILLE_CLIQUE_MINIMUM dans le graphe.
    Une clique est un groupe d'entités toutes reliées entre elles.
    Utilise un algorithme de Bron-Kerbosch simplifié via Cypher.
    """
    requete = f"""
        MATCH (e1:Entite)-[:PARTAGE_IP|COSIGNATURE|PARTAGE_ADRESSE]-(e2:Entite)
        WITH e1, collect(DISTINCT e2.identifiant) AS voisins
        WHERE size(voisins) >= {TAILLE_CLIQUE_MINIMUM - 1}
        RETURN e1.identifiant AS noeud, voisins
    """
    resultats = connexion.executer_requete_lecture(requete)

    # Construire les cliques candidates
    adjacence: Dict[str, set] = {}
    for r in resultats:
        adjacence[r["noeud"]] = set(r["voisins"])

    cliques_validees = []
    noeuds_potentiels = list(adjacence.keys())

    for noeud in noeuds_potentiels:
        clique_candidate = {noeud} | (adjacence.get(noeud, set()) & set(noeuds_potentiels))
        # Vérifier que tous les membres sont connectés entre eux
        valide = True
        membres = list(clique_candidate)
        for i in range(len(membres)):
            for j in range(i + 1, len(membres)):
                if membres[j] not in adjacence.get(membres[i], set()):
                    valide = False
                    break
            if not valide:
                break

        if valide and len(clique_candidate) >= TAILLE_CLIQUE_MINIMUM:
            cliques_validees.append(sorted(clique_candidate))

    # Dédupliquer
    cliques_uniques = list({tuple(c) for c in cliques_validees})
    cliques_listes  = [list(c) for c in cliques_uniques]

    journal.info(f"Cliques détectées : {len(cliques_listes)} (taille min={TAILLE_CLIQUE_MINIMUM})")
    return cliques_listes


# ================================================================
#  CENTRALITÉ DE PROXIMITÉ
# ================================================================

def calculer_centralite_proximite(
    connexion: ConnexionNeo4j
) -> Dict[str, float]:
    """
    Calcule la centralité de proximité (closeness centrality) pour
    chaque entité via BFS dans Neo4j.
    Retourne {identifiant: score_centralite}.
    """
    requete = """
        MATCH (source:Entite)
        CALL apoc.algo.closeness(['CAUSE_DE', 'IMPLIQUE'], source, 'OUTGOING')
        YIELD node, score
        RETURN node.identifiant AS identifiant, score
    """
    try:
        resultats = connexion.executer_requete_lecture(requete)
        centralites = {r["identifiant"]: r["score"] for r in resultats}
    except Exception:
        # Fallback si APOC n'est pas disponible : centralité approximative
        journal.warning("APOC non disponible, centralité approximée via PageRank")
        centralites = calculer_pagerank(connexion)

    return centralites


# ================================================================
#  PIPELINE MÉTRIQUES TOPOLOGIQUES
# ================================================================

def calculer_metriques_topologiques(connexion: ConnexionNeo4j) -> Dict:
    """
    Calcule l'ensemble des métriques topologiques du graphe.
    Retourne un résumé consolidé.
    """
    journal.info("Calcul des métriques topologiques")

    scores_pagerank  = calculer_pagerank(connexion)
    hubs             = detecter_hubs(connexion)
    cliques          = detecter_cliques(connexion)
    centralites      = calculer_centralite_proximite(connexion)

    resume = {
        "noeuds_pagerank":   len(scores_pagerank),
        "hubs_detectes":     len(hubs),
        "cliques_detectees": len(cliques),
        "noeuds_centralite": len(centralites),
    }
    journal.info(f"Métriques topologiques calculées : {resume}")
    return resume