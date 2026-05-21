import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from config.configuration import (
    SEUIL_CHEVAUCHEMENT_CONTINUITE,
    SEUIL_CHEVAUCHEMENT_TRANSITION,
    SEUIL_FRAGMENTATION_SUSPECTE,
    SEUIL_COHESION_DIFFUSE,
    SEUIL_COHESION_NORMALE,
    SEUIL_COHESION_DENSE,
    SEUIL_DENSITE_NOYAU_DUR,
    AGE_MOYEN_NOYAU_DUR_JOURS,
    SEUIL_CONFIANCE_INTER_FUSION,
)
from core.connexion_neo4j import ConnexionNeo4j, horodatage_actuel_utc

journal = logging.getLogger("Phase6.DetectionCommunautes")



#  ALGORITHME DE LOUVAIN SIMPLIFIÉ (sur graphe en mémoire)


def _construire_graphe_memoire(connexion: ConnexionNeo4j) -> Tuple[List[str], Dict[str, List[Tuple[str, float]]]]:
    """
    Charge le graphe depuis Neo4j en mémoire pour l'algorithme Louvain.
    Retourne (liste_noeuds, adjacence_ponderee).
    """
    requete = """
        MATCH (a:Entite)-[lien]->(b:Entite)
        WHERE lien.score_confiance IS NOT NULL
        RETURN
            a.identifiant AS source,
            b.identifiant AS cible,
            coalesce(lien.score_confiance, 0.5) AS poids
    """
    aretes  = connexion.executer_requete_lecture(requete)
    noeuds  = set()
    adjacence: Dict[str, List[Tuple[str, float]]] = defaultdict(list)

    for a in aretes:
        s, c, p = a["source"], a["cible"], a["poids"]
        noeuds.add(s)
        noeuds.add(c)
        adjacence[s].append((c, p))
        adjacence[c].append((s, p))  # Non-dirigé pour Louvain

    return list(noeuds), dict(adjacence)


def _calculer_modularite(
    communautes: Dict[str, int],
    adjacence: Dict[str, List[Tuple[str, float]]],
    poids_total: float
) -> float:
    """Calcule la modularité Q de la partition actuelle."""
    if poids_total == 0:
        return 0.0
    q = 0.0
    for noeud, com in communautes.items():
        ki = sum(p for _, p in adjacence.get(noeud, []))
        for voisin, poids in adjacence.get(noeud, []):
            if communautes.get(voisin) == com:
                kj = sum(p for _, p in adjacence.get(voisin, []))
                q += poids - (ki * kj) / (2 * poids_total)
    return q / (2 * poids_total)


def detecter_communautes_louvain(connexion: ConnexionNeo4j) -> Dict[str, int]:
    """
    Algorithme de Louvain simplifié : une passe de greedy modularity.
    Retourne {identifiant_entite: id_communaute}.
    """
    noeuds, adjacence = _construire_graphe_memoire(connexion)
    if not noeuds:
        journal.warning("Graphe vide — aucune communauté détectée")
        return {}

    # Initialisation : chaque nœud dans sa propre communauté
    communautes  = {n: i for i, n in enumerate(noeuds)}
    poids_total  = sum(p for voisins in adjacence.values() for _, p in voisins) / 2.0

    amelioration = True
    while amelioration:
        amelioration = False
        for noeud in noeuds:
            com_actuelle = communautes[noeud]
            # Compter les poids vers chaque communauté voisine
            poids_vers_com: Dict[int, float] = defaultdict(float)
            for voisin, poids in adjacence.get(noeud, []):
                poids_vers_com[communautes[voisin]] += poids

            # Trouver la meilleure communauté
            meilleure_com   = com_actuelle
            meilleur_gain   = 0.0
            for com, poids in poids_vers_com.items():
                gain = poids - poids_vers_com.get(com_actuelle, 0.0)
                if gain > meilleur_gain:
                    meilleur_gain = gain
                    meilleure_com = com

            if meilleure_com != com_actuelle:
                communautes[noeud] = meilleure_com
                amelioration = True

    # Renuméroter les communautés de 0 à N-1
    mapping   = {v: i for i, v in enumerate(sorted(set(communautes.values())))}
    communautes = {n: mapping[c] for n, c in communautes.items()}

    nb_communautes = len(set(communautes.values()))
    journal.info(f"Communautés détectées : {nb_communautes} pour {len(noeuds)} nœuds")
    _persister_communautes(communautes, connexion)
    return communautes


def _persister_communautes(
    communautes: Dict[str, int],
    connexion: ConnexionNeo4j
) -> None:
    """Enregistre l'appartenance communautaire dans Neo4j."""
    requete = """
        MATCH (e:Entite {identifiant: $identifiant})
        SET e.id_communaute = $id_communaute, e.date_communaute = datetime()
    """
    connexion.executer_transaction_multiple([
        (requete, {"identifiant": n, "id_communaute": c})
        for n, c in communautes.items()
    ])



#  COHÉSION ET CLASSIFICATION DES COMMUNAUTÉS


def calculer_cohesion(
    membres: List[str],
    adjacence: Dict[str, List[Tuple[str, float]]]
) -> float:
    """
    Calcule la cohésion interne d'une communauté.
    = ratio (liens internes réels) / (liens internes possibles).
    """
    n = len(membres)
    if n < 2:
        return 0.0
    liens_possibles = n * (n - 1) / 2
    membres_set     = set(membres)
    liens_reels     = sum(
        1
        for m in membres
        for v, _ in adjacence.get(m, [])
        if v in membres_set and v > m  # compter chaque lien une fois
    )
    return liens_reels / liens_possibles


def classifier_communaute(cohesion: float) -> str:
    """Classifie une communauté selon sa cohésion."""
    if cohesion < SEUIL_COHESION_DIFFUSE:
        return "DIFFUSE"
    if cohesion < SEUIL_COHESION_NORMALE:
        return "NORMALE"
    if cohesion < SEUIL_COHESION_DENSE:
        return "DENSE"
    return "TRES_DENSE"



#  DÉTECTION DE NOYAUX DURS


def detecter_noyau_dur(
    membres: List[str],
    connexion: ConnexionNeo4j
) -> List[str]:
    """
    Identifie le noyau dur d'une communauté :
    entités anciennes et fortement connectées entre elles.
    """
    if not membres:
        return []

    seuil_age = horodatage_actuel_utc() - timedelta(days=AGE_MOYEN_NOYAU_DUR_JOURS)
    requete = """
        MATCH (e:Entite)
        WHERE e.identifiant IN $membres
          AND e.date_creation <= datetime($seuil_age)
        RETURN e.identifiant AS identifiant
    """
    resultats = connexion.executer_requete_lecture(
        requete,
        {"membres": membres, "seuil_age": seuil_age.isoformat()}
    )
    candidats_anciens = [r["identifiant"] for r in resultats]

    if len(candidats_anciens) < 2:
        return candidats_anciens

    _, adjacence = _construire_graphe_memoire(connexion)
    cohesion_noyau = calculer_cohesion(candidats_anciens, adjacence)

    if cohesion_noyau >= SEUIL_DENSITE_NOYAU_DUR:
        journal.info(f"Noyau dur identifié : {len(candidats_anciens)} membres (cohésion={cohesion_noyau:.3f})")
        return candidats_anciens
    return []



#  SUIVI DE L'ÉVOLUTION TEMPORELLE


def analyser_evolution_communaute(
    id_communaute: int,
    connexion: ConnexionNeo4j
) -> str:
    """
    Compare la communauté actuelle à l'état précédent.
    Retourne le statut : STABLE, CROISSANTE, DECROISSANTE, FRAGMENTEE, FUSIONNEE.
    """
    requete_historique = """
        MATCH (e:Entite)
        WHERE e.id_communaute = $id_communaute
        RETURN
            count(e) AS taille_actuelle,
            avg(e.score_pagerank) AS score_moyen,
            collect(e.id_communaute_precedente) AS communautes_precedentes
    """
    resultats = connexion.executer_requete_lecture(
        requete_historique, {"id_communaute": id_communaute}
    )
    if not resultats:
        return "INCONNUE"

    r = resultats[0]
    taille_actuelle = r["taille_actuelle"]
    communautes_precedentes = [c for c in (r["communautes_precedentes"] or []) if c is not None]

    if not communautes_precedentes:
        return "NOUVELLE"

    nb_precedentes_uniques = len(set(communautes_precedentes))

    if nb_precedentes_uniques >= SEUIL_FRAGMENTATION_SUSPECTE:
        return "FRAGMENTEE_SUSPECTE"
    if nb_precedentes_uniques > 1:
        return "FUSIONNEE"

    # Calculer le chevauchement avec la communauté précédente
    chevauchement = len(communautes_precedentes) / max(1, taille_actuelle)
    if chevauchement >= SEUIL_CHEVAUCHEMENT_CONTINUITE:
        return "STABLE"
    if chevauchement >= SEUIL_CHEVAUCHEMENT_TRANSITION:
        return "EN_TRANSITION"
    return "REORGANISEE"



#  PIPELINE DÉTECTION DE COMMUNAUTÉS


def analyser_communautes(connexion: ConnexionNeo4j) -> Dict:
    """
    Pipeline complet : détection, classification, noyaux durs, évolution.
    Retourne un résumé analytique.
    """
    journal.info("Démarrage de la détection de communautés")

    communautes = detecter_communautes_louvain(connexion)
    if not communautes:
        return {"erreur": "Aucune communauté détectée"}

    # Grouper les membres par communauté
    groupes: Dict[int, List[str]] = defaultdict(list)
    for noeud, id_com in communautes.items():
        groupes[id_com].append(noeud)

    _, adjacence = _construire_graphe_memoire(connexion)

    statistiques = []
    for id_com, membres in groupes.items():
        cohesion      = calculer_cohesion(membres, adjacence)
        classification = classifier_communaute(cohesion)
        noyau         = detecter_noyau_dur(membres, connexion)
        evolution     = analyser_evolution_communaute(id_com, connexion)

        statistiques.append({
            "id_communaute":  id_com,
            "taille":         len(membres),
            "cohesion":       round(cohesion, 4),
            "classification": classification,
            "taille_noyau":   len(noyau),
            "evolution":      evolution,
        })

    resume = {
        "nb_communautes":   len(groupes),
        "communautes":      statistiques,
        "fragmentees":      sum(1 for s in statistiques if s["evolution"] == "FRAGMENTEE_SUSPECTE"),
    }
    journal.info(f"Analyse communautés terminée : {resume['nb_communautes']} communautés")
    return resume