import logging
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from config.configuration import (
    SEUIL_FUSION_ENTITES,
    DISTANCE_MAX_PREFILTRAGE,
    POIDS_SIMILARITE_NOM,
    POIDS_SIMILARITE_PHONETIQUE,
    POIDS_SIMILARITE_ADRESSE,
    POIDS_SIMILARITE_TELEPHONE,
    POIDS_SIMILARITE_EMAIL,
    POIDS_SIMILARITE_IP,
    POIDS_SIMILARITE_COMPTE,
    POIDS_SIMILARITE_SIRET,
    JOURS_IDENTITE_SYNTHETIQUE,
    NOMBRE_ENTITES_RECENTES_MIN,
    ATTRIBUTS_COMMUNS_MIN,
)
from core.connexion_neo4j import ConnexionNeo4j, horodatage_actuel_utc

journal = logging.getLogger("Phase2.ResolutionEntites")



#  FONCTIONS DE SIMILARITÉ


def _normaliser_chaine(chaine: str) -> str:
    """Normalise une chaîne : minuscules, sans accents, sans espaces superflus."""
    if not chaine:
        return ""
    nfd = unicodedata.normalize("NFD", chaine.lower().strip())
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _similarite_levenshtein(a: str, b: str) -> float:
    """
    Calcule la similarité de Levenshtein normalisée entre deux chaînes.
    Retourne un score entre 0.0 (différent) et 1.0 (identique).
    """
    a, b = _normaliser_chaine(a), _normaliser_chaine(b)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    longueur_max = max(len(a), len(b))
    # Matrice DP
    precedent = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        courant = [i]
        for j, cb in enumerate(b, 1):
            cout = 0 if ca == cb else 1
            courant.append(min(
                precedent[j] + 1,
                courant[j - 1] + 1,
                precedent[j - 1] + cout
            ))
        precedent = courant
    distance = precedent[len(b)]
    return 1.0 - distance / longueur_max


def _similarite_phonetique(a: str, b: str) -> float:
    """
    Approximation phonétique simplifiée (soundex-like).
    Compare les 4 premiers caractères normalisés.
    """
    a, b = _normaliser_chaine(a), _normaliser_chaine(b)
    if not a or not b:
        return 0.0
    prefixe_a = a[:4].ljust(4, '0')
    prefixe_b = b[:4].ljust(4, '0')
    correspondances = sum(1 for x, y in zip(prefixe_a, prefixe_b) if x == y)
    return correspondances / 4.0


def _similarite_exacte(a: str, b: str) -> float:
    """Retourne 1.0 si les deux chaînes normalisées sont identiques, sinon 0.0."""
    return 1.0 if _normaliser_chaine(a) == _normaliser_chaine(b) else 0.0



#  CALCUL DU SCORE DE SIMILARITÉ COMPOSITE


def calculer_score_similarite(entite_a: Dict, entite_b: Dict) -> float:
    """
    Calcule le score de similarité composite entre deux entités
    selon les 8 dimensions pondérées de la configuration.

    Retourne un score entre 0.0 et 1.0.
    """
    score = 0.0

    score += POIDS_SIMILARITE_NOM * _similarite_levenshtein(
        entite_a.get("nom", ""), entite_b.get("nom", "")
    )
    score += POIDS_SIMILARITE_PHONETIQUE * _similarite_phonetique(
        entite_a.get("nom", ""), entite_b.get("nom", "")
    )
    score += POIDS_SIMILARITE_ADRESSE * _similarite_levenshtein(
        entite_a.get("adresse", ""), entite_b.get("adresse", "")
    )
    score += POIDS_SIMILARITE_TELEPHONE * _similarite_exacte(
        entite_a.get("telephone", ""), entite_b.get("telephone", "")
    )
    score += POIDS_SIMILARITE_EMAIL * _similarite_exacte(
        entite_a.get("email", ""), entite_b.get("email", "")
    )
    score += POIDS_SIMILARITE_IP * _similarite_exacte(
        entite_a.get("adresse_ip", ""), entite_b.get("adresse_ip", "")
    )
    score += POIDS_SIMILARITE_COMPTE * _similarite_exacte(
        entite_a.get("numero_compte", ""), entite_b.get("numero_compte", "")
    )
    score += POIDS_SIMILARITE_SIRET * _similarite_exacte(
        entite_a.get("siret", ""), entite_b.get("siret", "")
    )

    return min(score, 1.0)



#  PRÉFILTRAGE RAPIDE


def _prefiltrer_candidats(
    entite_cible: Dict,
    catalogue: List[Dict]
) -> List[Dict]:
    """
    Élimine rapidement les entités trop dissemblables avant le calcul
    du score complet (optimisation O(n) → O(k) avec k << n).
    Utilise la distance sur le nom uniquement comme proxy rapide.
    """
    candidats = []
    nom_cible = _normaliser_chaine(entite_cible.get("nom", ""))
    for entite in catalogue:
        nom_candidat = _normaliser_chaine(entite.get("nom", ""))
        # Heuristique : différence de longueur
        if abs(len(nom_cible) - len(nom_candidat)) / max(len(nom_cible), 1) > DISTANCE_MAX_PREFILTRAGE * 3:
            continue
        # Préfixe commun minimal
        if nom_cible[:3] == nom_candidat[:3] or not nom_cible or not nom_candidat:
            candidats.append(entite)
        elif _similarite_levenshtein(nom_cible[:6], nom_candidat[:6]) > (1 - DISTANCE_MAX_PREFILTRAGE):
            candidats.append(entite)
    return candidats



#  DÉTECTION D'IDENTITÉ SYNTHÉTIQUE


def detecter_identite_synthetique(
    entite: Dict,
    connexion: ConnexionNeo4j
) -> bool:
    """
    Détecte un pattern d'identité synthétique :
    - Plusieurs entités récentes partageant des attributs similaires
    - Créées en rafale dans une fenêtre courte
    """
    maintenant  = horodatage_actuel_utc()
    seuil_date  = maintenant - timedelta(days=JOURS_IDENTITE_SYNTHETIQUE)

    requete = """
        MATCH (e:Entite)
        WHERE e.date_creation >= $seuil_date
          AND (e.email = $email OR e.adresse_ip = $adresse_ip
               OR e.telephone = $telephone)
          AND e.identifiant <> $identifiant
        RETURN count(e) AS nombre_similaires
    """
    parametres = {
        "seuil_date":   seuil_date.isoformat(),
        "email":        entite.get("email", ""),
        "adresse_ip":   entite.get("adresse_ip", ""),
        "telephone":    entite.get("telephone", ""),
        "identifiant":  entite.get("identifiant", ""),
    }
    resultats = connexion.executer_requete_lecture(requete, parametres)
    nombre    = resultats[0]["nombre_similaires"] if resultats else 0

    if nombre >= NOMBRE_ENTITES_RECENTES_MIN:
        journal.warning(
            f"Identité synthétique suspectée pour '{entite.get('identifiant')}' "
            f"({nombre} entités similaires récentes)"
        )
        return True
    return False



#  FUSION D'ENTITÉS


def fusionner_entites(
    entite_principale: Dict,
    entite_fusionnee: Dict,
    connexion: ConnexionNeo4j
) -> Dict:
    """
    Fusionne deux entités dans Neo4j.
    L'entité principale absorbe les attributs de l'entité fusionnée.
    Crée un lien FUSION_DE pour traçabilité.
    """
    requete_fusion = """
        MATCH (principale:Entite {identifiant: $id_principal})
        MATCH (fusionnee:Entite  {identifiant: $id_fusionne})
        CREATE (principale)-[:FUSION_DE {
            date_fusion:    datetime(),
            score_similarite: $score,
            motif:          'resolution_automatique'
        }]->(fusionnee)
        SET principale.identifiants_fusionnes =
            coalesce(principale.identifiants_fusionnes, []) + [$id_fusionne]
        DETACH DELETE fusionnee
        RETURN principale
    """
    score = calculer_score_similarite(entite_principale, entite_fusionnee)
    connexion.executer_requete_ecriture(requete_fusion, {
        "id_principal": entite_principale["identifiant"],
        "id_fusionne":  entite_fusionnee["identifiant"],
        "score":        score,
    })
    journal.info(
        f"Fusion : '{entite_fusionnee['identifiant']}' → '{entite_principale['identifiant']}' "
        f"(score={score:.3f})"
    )
    return entite_principale


def resoudre_entites(
    entites: List[Dict],
    connexion: ConnexionNeo4j
) -> List[Dict]:
    """
    Résout les entités en lot : détecte les doublons et les fusionne
    si le score dépasse le seuil de fusion.

    Retourne la liste des entités après résolution.
    """
    entites_resolues = list(entites)
    fusions          = 0

    i = 0
    while i < len(entites_resolues):
        cible     = entites_resolues[i]
        candidats = _prefiltrer_candidats(cible, entites_resolues[i + 1:])
        j = i + 1
        while j < len(entites_resolues):
            if entites_resolues[j] not in candidats:
                j += 1
                continue
            score = calculer_score_similarite(cible, entites_resolues[j])
            if score >= SEUIL_FUSION_ENTITES:
                fusionner_entites(cible, entites_resolues[j], connexion)
                entites_resolues.pop(j)
                fusions += 1
            else:
                j += 1
        i += 1

    journal.info(f"Résolution : {fusions} fusions effectuées, {len(entites_resolues)} entités restantes")
    return entites_resolues