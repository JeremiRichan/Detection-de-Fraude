# ================================================================
#  MAIN — POINT D'ENTRÉE DU PIPELINE DE DÉTECTION DE FRAUDE
#  Orchestration : génération dynamique + 8 phases d'analyse
# ================================================================

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

from core.connexion_neo4j       import ConnexionNeo4j
from core.generateur_donnees    import generer_et_inserer
from core.validation_ingestion  import ingerer_lot
from core.resolution_entites    import resoudre_entites
from core.construction_graphe   import construire_graphe
from core.metrique_topologique  import calculer_metriques_topologiques
from core.model_confiance       import calculer_confiances_liens
from core.detection_communautes import analyser_communautes
from core.scoring_decision      import scorer_lot
from core.evaluation_feedback   import evaluer_et_ajuster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
journal = logging.getLogger("Main")


# ================================================================
#  PIPELINE PRINCIPAL
# ================================================================

def executer_pipeline(
    connexion: ConnexionNeo4j,
    nb_entites: int = 1000,
    vider_existant: bool = False,
    seed: Optional[int] = None,
) -> Dict:
    debut = datetime.now(timezone.utc)
    journal.info("=" * 60)
    journal.info("DÉMARRAGE DU PIPELINE DE DÉTECTION DE FRAUDE")
    journal.info(f"Paramètres : {nb_entites} entités | vider={vider_existant} | seed={seed}")
    journal.info("=" * 60)

    resultats: Dict = {}

    # ── Étape 0 — Génération dynamique des données ───────────────
    journal.info("▶ Étape 0 : Génération dynamique des données")
    resume_gen, evenements_bruts = generer_et_inserer(
        connexion,
        nb_entites=nb_entites,
        vider_existant=vider_existant,
        seed=seed,
    )
    resultats["generation"] = resume_gen
    journal.info(
        f"   {resume_gen['entites_inserees']} entités insérées | "
        f"{resume_gen['relations_creees']} relations | "
        f"{resume_gen['evenements_generes']} événements bruts"
    )

    # Construire la liste d'entités pour les phases suivantes
    entites_brutes = _recuperer_entites_neo4j(connexion)

    # ── Phase 1 — Ingestion et validation ───────────────────────
    journal.info("▶ Phase 1 : Validation et ingestion des événements")
    evenements_valides, nb_rejets = ingerer_lot(evenements_bruts, connexion)
    resultats["phase1"] = {
        "total_bruts":  len(evenements_bruts),
        "acceptes":     len(evenements_valides),
        "rejetes":      nb_rejets,
        "taux_rejet":   round(nb_rejets / max(1, len(evenements_bruts)) * 100, 1),
    }

    # ── Phase 2 — Résolution d'entités ──────────────────────────
    journal.info("▶ Phase 2 : Résolution et fusion des entités")
    entites_resolues = resoudre_entites(entites_brutes, connexion)
    resultats["phase2"] = {
        "entites_avant":  len(entites_brutes),
        "entites_apres":  len(entites_resolues),
        "fusions":        len(entites_brutes) - len(entites_resolues),
    }

    # ── Phase 3 — Construction du graphe ────────────────────────
    journal.info("▶ Phase 3 : Construction du graphe causal")
    resume_graphe = construire_graphe(evenements_valides[:500], connexion)
    resultats["phase3"] = resume_graphe

    # ── Phase 4 — Métriques topologiques ────────────────────────
    journal.info("▶ Phase 4 : Métriques topologiques (PageRank, hubs, cliques)")
    resume_topologie = calculer_metriques_topologiques(connexion)
    resultats["phase4"] = resume_topologie

    # ── Phase 5 — Modèle de confiance ───────────────────────────
    journal.info("▶ Phase 5 : Calcul des scores de confiance")
    nb_liens = calculer_confiances_liens(connexion)
    resultats["phase5"] = {"liens_traites": nb_liens}

    # ── Phase 6 — Détection de communautés ──────────────────────
    journal.info("▶ Phase 6 : Détection de communautés (Louvain)")
    resume_com = analyser_communautes(connexion)
    resultats["phase6"] = resume_com

    # ── Phase 7 — Scoring et décision ───────────────────────────
    journal.info("▶ Phase 7 : Scoring de risque (Isolation Forest + GNN)")
    ids_entites = [e["identifiant"] for e in entites_resolues[:200]]
    scores      = scorer_lot(ids_entites, connexion)
    nb_suspects = sum(1 for s in scores if s["decision"] == "FRAUDE_SUSPECTEE")
    resultats["phase7"] = {
        "entites_scorees": len(scores),
        "suspects":        nb_suspects,
        "normaux":         len(scores) - nb_suspects,
        "taux_detection":  round(nb_suspects / max(1, len(scores)) * 100, 1),
    }

    # ── Phase 8 — Évaluation et feedback ────────────────────────
    journal.info("▶ Phase 8 : Évaluation des performances et feedback")
    resume_eval = evaluer_et_ajuster(connexion)
    resultats["phase8"] = resume_eval

    # ── Résumé final ─────────────────────────────────────────────
    duree = (datetime.now(timezone.utc) - debut).total_seconds()
    resultats["duree_totale_secondes"] = round(duree, 2)

    journal.info("=" * 60)
    journal.info(f"PIPELINE TERMINÉ en {duree:.1f}s")
    journal.info(f"  Entités analysées : {len(entites_resolues)}")
    journal.info(f"  Suspects détectés : {nb_suspects} ({resultats['phase7']['taux_detection']}%)")
    f1 = resume_eval.get("metriques", {}).get("f1", "N/A")
    journal.info(f"  F1 Score          : {f1}")
    journal.info("=" * 60)

    return resultats


def _recuperer_entites_neo4j(connexion: ConnexionNeo4j) -> List[Dict]:
    """Récupère toutes les entités depuis Neo4j pour les phases suivantes."""
    requete = """
        MATCH (e:Entite)
        RETURN
            e.identifiant  AS identifiant,
            e.nom          AS nom,
            e.email        AS email,
            e.telephone    AS telephone,
            e.adresse      AS adresse,
            e.adresse_ip   AS adresse_ip,
            e.siret        AS siret,
            e.numero_compte AS numero_compte,
            e.categorie    AS categorie
        LIMIT 2000
    """
    return connexion.executer_requete_lecture(requete)


# ================================================================
#  ARGUMENTS EN LIGNE DE COMMANDE
# ================================================================

def _parser_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline de détection de fraude sur graphe Neo4j"
    )
    parser.add_argument(
        "--nb-entites", type=int, default=1000,
        help="Nombre d'entités à générer (défaut : 1000)"
    )
    parser.add_argument(
        "--vider", action="store_true",
        help="Vider la base Neo4j avant la génération"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Graine aléatoire pour reproductibilité (défaut : aléatoire)"
    )
    parser.add_argument(
        "--generer-seulement", action="store_true",
        help="Générer les données uniquement, sans lancer le pipeline"
    )
    return parser.parse_args()


# ================================================================
#  POINT D'ENTRÉE
# ================================================================

def main() -> int:
    args = _parser_arguments()

    try:
        with ConnexionNeo4j() as connexion:
            if args.generer_seulement:
                resume, _ = generer_et_inserer(
                    connexion,
                    nb_entites=args.nb_entites,
                    vider_existant=args.vider,
                    seed=args.seed,
                )
                journal.info(f"Données générées : {resume}")
            else:
                resultats = executer_pipeline(
                    connexion,
                    nb_entites=args.nb_entites,
                    vider_existant=args.vider,
                    seed=args.seed,
                )
                journal.info(f"Pipeline complet : {resultats}")
        return 0

    except Exception as erreur:
        journal.error(f"Erreur critique : {erreur}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())