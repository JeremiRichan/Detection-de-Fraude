import logging
import math
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from config.configuration import (
    PROPORTION_ENTRAINEMENT,
    NOMBRE_CARACTERISTIQUES_DERIVE,
    SEUIL_REENTRAINEMENT_LABELS,
    INCREMENT_RISQUE_VOISIN,
    INCREMENT_RISQUE_COMMUNAUTE,
    DECREMENT_FAUX_POSITIF,
    SEUIL_COHESION_PROPAGATION,
    SEUIL_DISPARITE_BIAIS_MAX,
)
from core.connexion_neo4j import ConnexionNeo4j, horodatage_actuel_utc

journal = logging.getLogger("Phase8.EvaluationFeedback")



#  MÉTRIQUES DE CLASSIFICATION


def calculer_metriques(
    vrais_positifs: int,
    faux_positifs: int,
    faux_negatifs: int,
    vrais_negatifs: int
) -> Dict[str, float]:
    """
    Calcule les métriques standard de classification binaire.
    Gère les divisions par zéro.
    """
    total = vrais_positifs + faux_positifs + faux_negatifs + vrais_negatifs

    precision = (
        vrais_positifs / (vrais_positifs + faux_positifs)
        if (vrais_positifs + faux_positifs) > 0 else 0.0
    )
    rappel = (
        vrais_positifs / (vrais_positifs + faux_negatifs)
        if (vrais_positifs + faux_negatifs) > 0 else 0.0
    )
    f1 = (
        2 * precision * rappel / (precision + rappel)
        if (precision + rappel) > 0 else 0.0
    )
    accuracy = (
        (vrais_positifs + vrais_negatifs) / total
        if total > 0 else 0.0
    )
    # Spécificité
    specificite = (
        vrais_negatifs / (vrais_negatifs + faux_positifs)
        if (vrais_negatifs + faux_positifs) > 0 else 0.0
    )
    # Matthews Correlation Coefficient
    denominateur_mcc = math.sqrt(
        (vrais_positifs + faux_positifs)
        * (vrais_positifs + faux_negatifs)
        * (vrais_negatifs + faux_positifs)
        * (vrais_negatifs + faux_negatifs)
    )
    mcc = (
        (vrais_positifs * vrais_negatifs - faux_positifs * faux_negatifs) / denominateur_mcc
        if denominateur_mcc > 0 else 0.0
    )

    return {
        "precision":    round(precision,    4),
        "rappel":       round(rappel,        4),
        "f1":           round(f1,            4),
        "accuracy":     round(accuracy,      4),
        "specificite":  round(specificite,   4),
        "mcc":          round(mcc,           4),
        "vp": vrais_positifs,
        "fp": faux_positifs,
        "fn": faux_negatifs,
        "vn": vrais_negatifs,
    }


def evaluer_modele(connexion: ConnexionNeo4j) -> Dict:
    """
    Évalue les performances du modèle en comparant les prédictions
    aux labels de vérité terrain disponibles dans Neo4j.
    Retourne les métriques consolidées.
    """
    requete = """
        MATCH (e:Entite)
        WHERE e.label_verite IS NOT NULL
          AND e.decision_fraude IS NOT NULL
        RETURN
            e.label_verite    AS label,
            e.decision_fraude AS prediction,
            e.score_risque    AS score
        ORDER BY rand()
    """
    donnees = connexion.executer_requete_lecture(requete)
    if not donnees:
        journal.warning("Aucune donnée labellisée disponible pour évaluation")
        return {}

    n_train = int(len(donnees) * PROPORTION_ENTRAINEMENT)
    donnees_test = donnees[n_train:]

    vp = fp = fn = vn = 0
    for d in donnees_test:
        est_fraude_reel  = d["label"]      == "FRAUDE"
        est_fraude_predit = d["prediction"] == "FRAUDE_SUSPECTEE"
        if est_fraude_reel  and est_fraude_predit:  vp += 1
        if not est_fraude_reel and est_fraude_predit:  fp += 1
        if est_fraude_reel  and not est_fraude_predit: fn += 1
        if not est_fraude_reel and not est_fraude_predit: vn += 1

    metriques = calculer_metriques(vp, fp, fn, vn)
    metriques["taille_test"]  = len(donnees_test)
    metriques["taille_train"] = n_train

    journal.info(
        f"Évaluation : F1={metriques['f1']:.4f}, "
        f"Précision={metriques['precision']:.4f}, "
        f"Rappel={metriques['rappel']:.4f}"
    )
    return metriques


#  PROPAGATION DU FEEDBACK

def propager_feedback_voisins(
    entite_id: str,
    label_confirme: str,
    connexion: ConnexionNeo4j
) -> int:
    """
    Propage le feedback d'un label confirmé aux voisins directs.
    - FRAUDE confirmée → incrément de risque pour les voisins
    - FAUX POSITIF → décrément de risque
    Retourne le nombre de voisins mis à jour.
    """
    est_fraude = label_confirme == "FRAUDE"
    delta      = INCREMENT_RISQUE_VOISIN if est_fraude else -DECREMENT_FAUX_POSITIF

    requete = """
        MATCH (e:Entite {identifiant: $id})-[]-(voisin:Entite)
        SET voisin.score_risque = min(1.0, max(0.0,
            coalesce(voisin.score_risque, 0.1) + $delta
        ))
        RETURN count(voisin) AS nb_voisins
    """
    resultats = connexion.executer_requete_ecriture(requete, {
        "id":    entite_id,
        "delta": delta,
    })
    nb = resultats[0]["nb_voisins"] if resultats else 0
    journal.info(f"Propagation feedback '{entite_id}' ({label_confirme}) : {nb} voisins mis à jour")
    return nb


def propager_feedback_communaute(
    entite_id: str,
    label_confirme: str,
    connexion: ConnexionNeo4j
) -> int:
    """
    Propage le feedback à toute la communauté si la cohésion est suffisante.
    Retourne le nombre de membres mis à jour.
    """
    requete_com = """
        MATCH (e:Entite {identifiant: $id})
        WHERE e.id_communaute IS NOT NULL
          AND e.cohesion_communaute >= $seuil_cohesion
        MATCH (membre:Entite {id_communaute: e.id_communaute})
        WHERE membre.identifiant <> $id
        SET membre.score_risque = min(1.0, max(0.0,
            coalesce(membre.score_risque, 0.1) + $delta
        ))
        RETURN count(membre) AS nb_membres
    """
    est_fraude = label_confirme == "FRAUDE"
    delta      = INCREMENT_RISQUE_COMMUNAUTE if est_fraude else -DECREMENT_FAUX_POSITIF / 2

    resultats = connexion.executer_requete_ecriture(requete_com, {
        "id":              entite_id,
        "seuil_cohesion":  SEUIL_COHESION_PROPAGATION,
        "delta":           delta,
    })
    nb = resultats[0]["nb_membres"] if resultats else 0
    journal.info(f"Propagation communauté '{entite_id}' : {nb} membres mis à jour")
    return nb


def enregistrer_label(
    entite_id: str,
    label: str,
    source: str,
    connexion: ConnexionNeo4j
) -> None:
    """
    Enregistre un label de vérité terrain dans Neo4j.
    Déclenche la propagation du feedback.
    """
    if label not in ("FRAUDE", "NORMAL", "FAUX_POSITIF"):
        raise ValueError(f"Label invalide : '{label}'. Valeurs acceptées : FRAUDE, NORMAL, FAUX_POSITIF")

    requete = """
        MATCH (e:Entite {identifiant: $id})
        SET e.label_verite      = $label,
            e.source_label      = $source,
            e.date_label        = datetime()
    """
    connexion.executer_requete_ecriture(requete, {
        "id":     entite_id,
        "label":  label,
        "source": source,
    })

    # Propager le feedback
    propager_feedback_voisins(entite_id, label, connexion)
    propager_feedback_communaute(entite_id, label, connexion)



#  DÉTECTION DE BIAIS (équité algorithmique)


def detecter_biais(connexion: ConnexionNeo4j) -> Dict:
    """
    Détecte les disparités de décision entre groupes d'entités.
    Compare le taux de détection par segment (type, communauté, etc.).
    Retourne un rapport de biais.
    """
    requete = """
        MATCH (e:Entite)
        WHERE e.decision_fraude IS NOT NULL
          AND e.segment IS NOT NULL
        WITH e.segment AS segment,
             count(e) AS total,
             sum(CASE WHEN e.decision_fraude = 'FRAUDE_SUSPECTEE' THEN 1 ELSE 0 END) AS nb_fraudes
        RETURN segment, total, nb_fraudes,
               toFloat(nb_fraudes) / total AS taux_detection
        ORDER BY taux_detection DESC
    """
    resultats = connexion.executer_requete_lecture(requete)
    if len(resultats) < 2:
        return {"biais_detecte": False, "nb_segments": len(resultats)}

    taux = [r["taux_detection"] for r in resultats]
    taux_max = max(taux)
    taux_min = min(taux)

    ratio_disparite = taux_max / taux_min if taux_min > 0 else float("inf")
    biais_detecte   = ratio_disparite > SEUIL_DISPARITE_BIAIS_MAX

    if biais_detecte:
        journal.warning(
            f"Biais détecté : ratio_disparité={ratio_disparite:.2f} > {SEUIL_DISPARITE_BIAIS_MAX}"
        )

    return {
        "biais_detecte":    biais_detecte,
        "ratio_disparite":  round(ratio_disparite, 4),
        "seuil":            SEUIL_DISPARITE_BIAIS_MAX,
        "segments":         resultats,
    }



#  DÉCLENCHEMENT DU RÉENTRAÎNEMENT


def verifier_reentrainement(connexion: ConnexionNeo4j) -> bool:
    """
    Vérifie si le seuil de labels accumulés justifie un réentraînement.
    Retourne True si le réentraînement doit être déclenché.
    """
    requete = """
        MATCH (e:Entite)
        WHERE e.label_verite IS NOT NULL
          AND e.utilise_entrainement IS NULL
        RETURN count(e) AS nb_nouveaux_labels
    """
    resultats = connexion.executer_requete_lecture(requete)
    nb = resultats[0]["nb_nouveaux_labels"] if resultats else 0

    if nb >= SEUIL_REENTRAINEMENT_LABELS:
        journal.info(f"Réentraînement déclenché : {nb} nouveaux labels (seuil={SEUIL_REENTRAINEMENT_LABELS})")
        return True
    return False



#  PIPELINE ÉVALUATION COMPLÈTE


def evaluer_et_ajuster(connexion: ConnexionNeo4j) -> Dict:
    """
    Pipeline complet Phase 8 :
    1. Évaluation des métriques du modèle
    2. Détection de biais
    3. Vérification du réentraînement
    """
    journal.info("Démarrage Phase 8 — Évaluation et feedback")

    metriques        = evaluer_modele(connexion)
    rapport_biais    = detecter_biais(connexion)
    reentrainement   = verifier_reentrainement(connexion)

    resume = {
        "metriques":            metriques,
        "biais":                rapport_biais,
        "reentrainement_requis": reentrainement,
    }
    journal.info(f"Phase 8 terminée : F1={metriques.get('f1', 'N/A')}, biais={rapport_biais.get('biais_detecte', False)}")
    return resume