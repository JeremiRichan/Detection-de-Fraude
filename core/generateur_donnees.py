# ================================================================
#  GÉNÉRATEUR DE DONNÉES DYNAMIQUES
#  Génère 1000 entités et leurs relations de façon réaliste
#  pour alimenter le pipeline de détection de fraude.
#
#  Patterns simulés :
#  - Entités normales (75%)
#  - Entités suspectes isolées (15%)
#  - Réseaux de fraude organisés en cliques (10%)
#  - Identités synthétiques (attributs partagés)
# ================================================================

import logging
import random
import string
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from core.connexion_neo4j import ConnexionNeo4j

journal = logging.getLogger("GenerateurDonnees")

# ── Paramètres de génération ─────────────────────────────────────
NB_ENTITES                  = 1000
NB_TRANSACTIONS_PAR_ENTITE  = (1, 8)       # min, max
RATIO_SUSPECTS              = 0.15
RATIO_RESEAU_FRAUDE         = 0.10
NB_RESEAUX_FRAUDE           = 5            # cliques organisées
TAILLE_RESEAU_FRAUDE        = (4, 12)      # membres par réseau
MONTANT_NORMAL              = (10, 5000)
MONTANT_SUSPECT             = (8000, 200000)
JOURS_HISTORIQUE            = 28    # < 30j : cohérent avec DUREE_RETRODATE_MAXIMUM_JOURS


# ================================================================
#  DONNÉES DE BASE POUR LA GÉNÉRATION
# ================================================================

PRENOMS = [
    "Jean", "Marie", "Pierre", "Sophie", "Paul", "Claire", "Louis",
    "Emma", "Thomas", "Julie", "Nicolas", "Léa", "Antoine", "Camille",
    "François", "Alice", "Julien", "Chloé", "Maxime", "Laura",
    "Alexandre", "Manon", "Romain", "Sarah", "Clément", "Inès",
    "Théo", "Lucie", "Baptiste", "Anaïs", "Hugo", "Mathilde",
    "Lucas", "Charlotte", "Arthur", "Océane", "Enzo", "Valentine",
    "Nathan", "Pauline", "Alexis", "Margot", "Raphaël", "Elisa",
]

NOMS = [
    "Martin", "Bernard", "Thomas", "Petit", "Robert", "Richard",
    "Durand", "Dupont", "Moreau", "Simon", "Laurent", "Michel",
    "Lefebvre", "Leroy", "Roux", "David", "Bertrand", "Morel",
    "Fournier", "Girard", "Bonnet", "Dupuis", "Lambert", "Fontaine",
    "Rousseau", "Vincent", "Muller", "Lefevre", "Faure", "Andre",
    "Mercier", "Blanc", "Guerin", "Boyer", "Garnier", "Chevalier",
    "Francois", "Legrand", "Gauthier", "Garcia", "Perrin", "Robin",
    "Clement", "Morin", "Nicolas", "Henry", "Roussel", "Mathieu",
]

DOMAINES_EMAIL = [
    "gmail.com", "yahoo.fr", "hotmail.com", "orange.fr",
    "free.fr", "sfr.fr", "outlook.com", "laposte.net",
    "wanadoo.fr", "bouyguestelecom.fr", "live.fr", "protonmail.com",
]

VILLES = [
    ("Paris", "75", "Île-de-France"),
    ("Lyon", "69", "Auvergne-Rhône-Alpes"),
    ("Marseille", "13", "Provence-Alpes-Côte d'Azur"),
    ("Toulouse", "31", "Occitanie"),
    ("Bordeaux", "33", "Nouvelle-Aquitaine"),
    ("Nantes", "44", "Pays de la Loire"),
    ("Lille", "59", "Hauts-de-France"),
    ("Strasbourg", "67", "Grand Est"),
    ("Rennes", "35", "Bretagne"),
    ("Grenoble", "38", "Auvergne-Rhône-Alpes"),
    ("Montpellier", "34", "Occitanie"),
    ("Nice", "06", "Provence-Alpes-Côte d'Azur"),
    ("Toulon", "83", "Provence-Alpes-Côte d'Azur"),
    ("Dijon", "21", "Bourgogne-Franche-Comté"),
    ("Angers", "49", "Pays de la Loire"),
]

TYPES_ENTITES = ["PERSONNE", "ENTREPRISE", "COMPTE_BANCAIRE", "MARCHAND"]
SEGMENTS      = ["RETAIL", "CORPORATE", "PME", "PARTICULIER", "PROFESSIONNEL"]

TYPES_RELATIONS = [
    "CAUSE_DE", "PARTAGE_IP", "COSIGNATURE", "PARTAGE_ADRESSE"
]

TYPES_EVENEMENTS = [
    "TRANSACTION", "EMAIL", "SMS", "LOG_ACCES", "APPROBATION"
]


# ================================================================
#  FONCTIONS UTILITAIRES
# ================================================================

def _id_aleatoire(prefixe: str, n: int = 8) -> str:
    return f"{prefixe}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=n))}"


def _ip_aleatoire(suspect: bool = False) -> str:
    if suspect:
        # IPs dans des plages souvent associées à des proxies/VPN
        prefixes = ["185.220", "45.142", "91.108", "194.165", "89.248"]
        return f"{random.choice(prefixes)}.{random.randint(1,254)}.{random.randint(1,254)}"
    return f"{random.randint(10,192)}.{random.randint(0,254)}.{random.randint(0,254)}.{random.randint(1,254)}"


def _telephone_aleatoire() -> str:
    prefixes = ["06", "07"]
    return f"+33{random.choice(prefixes)}{''.join([str(random.randint(0,9)) for _ in range(8)])}"


def _siret_aleatoire() -> str:
    return ''.join([str(random.randint(0, 9)) for _ in range(14)])


def _horodatage_aleatoire(jours_max: int = JOURS_HISTORIQUE) -> datetime:
    delta = timedelta(
        days=random.randint(0, jours_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return datetime.now(timezone.utc) - delta


def _montant_aleatoire(suspect: bool = False) -> float:
    if suspect:
        base = random.uniform(*MONTANT_SUSPECT)
        # Parfois des montants ronds (typique des mules)
        if random.random() < 0.3:
            base = round(base / 1000) * 1000
        return round(base, 2)
    return round(random.uniform(*MONTANT_NORMAL), 2)


def _adresse_aleatoire(ville_tuple: tuple) -> str:
    ville, dept, region = ville_tuple
    numero = random.randint(1, 150)
    rues = ["rue de la Paix", "avenue Victor Hugo", "boulevard Saint-Michel",
            "rue du Commerce", "allée des Roses", "impasse du Moulin",
            "chemin des Vignes", "rue Nationale", "avenue de la République",
            "place du Général de Gaulle"]
    return f"{numero} {random.choice(rues)}, {dept}000 {ville}"


# ================================================================
#  GÉNÉRATION DES ENTITÉS
# ================================================================

def generer_entites(nb: int = NB_ENTITES) -> List[Dict]:
    """
    Génère nb entités avec des profils variés :
    - normales, suspectes, appartenant à des réseaux de fraude
    """
    entites = []
    nb_suspects = int(nb * RATIO_SUSPECTS)
    nb_fraude   = int(nb * RATIO_RESEAU_FRAUDE)
    nb_normaux  = nb - nb_suspects - nb_fraude

    # ── IPs partagées pour simulation de réseaux ─────────────────
    ips_reseaux = [_ip_aleatoire(suspect=True) for _ in range(NB_RESEAUX_FRAUDE)]
    adresses_reseaux = [_adresse_aleatoire(random.choice(VILLES)) for _ in range(NB_RESEAUX_FRAUDE)]

    def _creer_entite(categorie: str, reseau_id: Optional[int] = None) -> Dict:
        prenom    = random.choice(PRENOMS)
        nom       = random.choice(NOMS)
        ville     = random.choice(VILLES)
        type_ent  = random.choice(TYPES_ENTITES)
        est_suspect = categorie in ("SUSPECT", "FRAUDE")

        # Les entités d'un même réseau partagent IP et/ou adresse
        if reseau_id is not None:
            adresse_ip  = ips_reseaux[reseau_id % NB_RESEAUX_FRAUDE]
            adresse     = adresses_reseaux[reseau_id % NB_RESEAUX_FRAUDE]
        else:
            adresse_ip  = _ip_aleatoire(suspect=est_suspect)
            adresse     = _adresse_aleatoire(ville)

        date_creation = _horodatage_aleatoire(jours_max=365 * 3)

        return {
            "identifiant":          _id_aleatoire("ENT"),
            "nom":                  f"{prenom} {nom}",
            "prenom":               prenom,
            "nom_famille":          nom,
            "email":                f"{prenom.lower()}.{nom.lower()}{random.randint(1,99)}@{random.choice(DOMAINES_EMAIL)}",
            "telephone":            _telephone_aleatoire(),
            "adresse":              adresse,
            "adresse_ip":           adresse_ip,
            "numero_compte":        _id_aleatoire("CPT", 10),
            "siret":                _siret_aleatoire() if type_ent == "ENTREPRISE" else None,
            "type_entite":          type_ent,
            "segment":              random.choice(SEGMENTS),
            "categorie":            categorie,
            "id_reseau":            reseau_id,
            "score_risque":         round(random.uniform(0.6, 0.95), 4) if est_suspect else round(random.uniform(0.0, 0.35), 4),
            "date_creation":        date_creation.isoformat(),
            "ville":                ville[0],
            "montant_moyen":        _montant_aleatoire(suspect=est_suspect),
            "montant_ecart_type":   round(random.uniform(100, 5000) if est_suspect else random.uniform(10, 500), 2),
            "nb_communautes":       random.randint(1, 3) if est_suspect else 1,
            "est_hub":              False,
            "identite_synthetique": categorie == "FRAUDE" and random.random() < 0.4,
            "score_pagerank":       0.0,
            "label_verite":         "FRAUDE" if est_suspect and random.random() < 0.6 else None,
            "segment_modele":       random.choice(["A", "B", "C"]),
        }

    # Entités normales
    for _ in range(nb_normaux):
        entites.append(_creer_entite("NORMAL"))

    # Entités suspectes isolées
    for _ in range(nb_suspects):
        entites.append(_creer_entite("SUSPECT"))

    # Entités dans des réseaux de fraude organisés
    taille_par_reseau = nb_fraude // NB_RESEAUX_FRAUDE
    for reseau_id in range(NB_RESEAUX_FRAUDE):
        for _ in range(taille_par_reseau):
            entites.append(_creer_entite("FRAUDE", reseau_id=reseau_id))

    random.shuffle(entites)
    journal.info(f"Entités générées : {len(entites)} (normales={nb_normaux}, suspectes={nb_suspects}, fraude={nb_fraude})")
    return entites


# ================================================================
#  GÉNÉRATION DES ÉVÉNEMENTS / RELATIONS
# ================================================================

def generer_evenements(entites: List[Dict]) -> List[Dict]:
    """
    Génère des événements (transactions, emails, etc.) reliant les entités.
    Les entités suspectes génèrent plus d'événements et de montants anormaux.
    """
    evenements = []
    ids = [e["identifiant"] for e in entites]

    for entite in entites:
        est_suspect = entite["categorie"] in ("SUSPECT", "FRAUDE")
        nb_evt = random.randint(
            NB_TRANSACTIONS_PAR_ENTITE[0],
            NB_TRANSACTIONS_PAR_ENTITE[1] * (3 if est_suspect else 1)
        )

        for _ in range(nb_evt):
            cible_id = random.choice(ids)
            while cible_id == entite["identifiant"]:
                cible_id = random.choice(ids)

            type_evt = random.choices(
                TYPES_EVENEMENTS,
                weights=[40, 20, 15, 15, 10] if not est_suspect else [55, 15, 10, 10, 10]
            )[0]

            horodatage = _horodatage_aleatoire()

            evenements.append({
                "id_source":           _id_aleatoire("EVT"),
                "type_evenement":      type_evt,
                "type_source":         random.choice(["INTERNE", "EXTERNE"]),
                "horodatage":          horodatage.isoformat(),
                "montant":             _montant_aleatoire(suspect=est_suspect),
                "id_entite_source":    entite["identifiant"],
                "id_entite_cible":     cible_id,
                "score_corroboration": round(random.uniform(0.3, 1.0) if not est_suspect else random.uniform(0.1, 0.7), 3),
                "nombre_sources":      random.randint(1, 3),
                "saut_hierarchique":   random.randint(0, 2),
                "adresse_ip":          entite["adresse_ip"],
            })

    # Relations supplémentaires au sein des réseaux de fraude
    entites_fraude = [e for e in entites if e.get("id_reseau") is not None]
    for entite in entites_fraude:
        membres_reseau = [
            e for e in entites_fraude
            if e.get("id_reseau") == entite.get("id_reseau")
            and e["identifiant"] != entite["identifiant"]
        ]
        for cible in random.sample(membres_reseau, min(3, len(membres_reseau))):
            evenements.append({
                "id_source":           _id_aleatoire("EVT"),
                "type_evenement":      "TRANSACTION",
                "type_source":         "INTERNE",
                "horodatage":          _horodatage_aleatoire(jours_max=30).isoformat(),
                "montant":             _montant_aleatoire(suspect=True),
                "id_entite_source":    entite["identifiant"],
                "id_entite_cible":     cible["identifiant"],
                "score_corroboration": round(random.uniform(0.1, 0.5), 3),
                "nombre_sources":      1,
                "saut_hierarchique":   0,
                "adresse_ip":          entite["adresse_ip"],
            })

    journal.info(f"Événements générés : {len(evenements)}")
    return evenements


# ================================================================
#  INSERTION DANS NEO4J EN MASSE (batch UNWIND)
# ================================================================

def inserer_entites_neo4j(
    entites: List[Dict],
    connexion: ConnexionNeo4j,
    taille_batch: int = 100
) -> int:
    """
    Insère les entités dans Neo4j par lots de taille_batch.
    Retourne le nombre d'entités insérées.
    """
    requete = """
        UNWIND $entites AS e
        MERGE (n:Entite {identifiant: e.identifiant})
        SET
            n.nom                  = e.nom,
            n.email                = e.email,
            n.telephone            = e.telephone,
            n.adresse              = e.adresse,
            n.adresse_ip           = e.adresse_ip,
            n.numero_compte        = e.numero_compte,
            n.siret                = e.siret,
            n.type_entite          = e.type_entite,
            n.segment              = e.segment,
            n.categorie            = e.categorie,
            n.id_reseau            = e.id_reseau,
            n.score_risque         = e.score_risque,
            n.date_creation        = datetime(e.date_creation),
            n.ville                = e.ville,
            n.montant_moyen        = e.montant_moyen,
            n.montant_ecart_type   = e.montant_ecart_type,
            n.nb_communautes       = e.nb_communautes,
            n.identite_synthetique = e.identite_synthetique,
            n.score_pagerank       = e.score_pagerank,
            n.label_verite         = e.label_verite,
            n.segment_modele       = e.segment_modele,
            n.score_risque_precedent = e.score_risque,
            n.cohesion_communaute  = 0.0,
            n.nb_hubs_voisins      = 0,
            n.score_corroboration_moyen = 0.5
        RETURN count(n) AS inseres
    """
    total = 0
    for i in range(0, len(entites), taille_batch):
        lot = entites[i:i + taille_batch]
        resultats = connexion.executer_requete_ecriture(requete, {"entites": lot})
        inseres = resultats[0]["inseres"] if resultats else 0
        total += inseres
        journal.info(f"  Entités insérées : {total}/{len(entites)}")

    return total


def inserer_relations_neo4j(
    entites: List[Dict],
    connexion: ConnexionNeo4j,
    taille_batch: int = 300
) -> int:
    """
    Génère et insère les relations entre entités par lots UNWIND.
    Évite le produit cartésien en passant les paires explicitement depuis Python.
    Retourne le nombre de relations créées.
    """
    total = 0

    # ── Construire les paires en mémoire Python (pas de cartesian product) ──
    # Grouper par adresse_ip
    groupes_ip: Dict[str, List[str]] = defaultdict(list)
    groupes_adr: Dict[str, List[str]] = defaultdict(list)
    groupes_reseau: Dict[int, List[str]] = defaultdict(list)

    for e in entites:
        if e.get("adresse_ip"):
            groupes_ip[e["adresse_ip"]].append(e["identifiant"])
        if e.get("adresse"):
            groupes_adr[e["adresse"]].append(e["identifiant"])
        if e.get("id_reseau") is not None:
            groupes_reseau[e["id_reseau"]].append(e["identifiant"])

    # ── Relations PARTAGE_IP ──────────────────────────────────────
    journal.info("Création des relations PARTAGE_IP...")
    paires_ip = []
    for ids in groupes_ip.values():
        if len(ids) < 2:
            continue
        ids_tries = sorted(ids)
        for i in range(len(ids_tries)):
            for j in range(i + 1, len(ids_tries)):
                paires_ip.append({
                    "source_id":       ids_tries[i],
                    "cible_id":        ids_tries[j],
                    "score_confiance": round(random.uniform(0.4, 0.9), 4),
                    "jours_offset":    random.randint(0, 28),
                })

    requete_ip = """
        UNWIND $paires AS p
        MATCH (a:Entite {identifiant: p.source_id})
        MATCH (b:Entite {identifiant: p.cible_id})
        MERGE (a)-[r:PARTAGE_IP]->(b)
        ON CREATE SET
            r.score_confiance = p.score_confiance,
            r.date_creation   = datetime() - duration({days: p.jours_offset}),
            r.inactif         = false
        RETURN count(r) AS nb
    """
    nb_ip = 0
    for i in range(0, len(paires_ip), taille_batch):
        res = connexion.executer_requete_ecriture(requete_ip, {"paires": paires_ip[i:i+taille_batch]})
        nb_ip += res[0]["nb"] if res else 0
    total += nb_ip
    journal.info(f"  PARTAGE_IP : {nb_ip} relations")

    # ── Relations PARTAGE_ADRESSE ─────────────────────────────────
    journal.info("Création des relations PARTAGE_ADRESSE...")
    paires_adr = []
    for ids in groupes_adr.values():
        if len(ids) < 2:
            continue
        ids_tries = sorted(ids)
        for i in range(len(ids_tries)):
            for j in range(i + 1, len(ids_tries)):
                paires_adr.append({
                    "source_id":       ids_tries[i],
                    "cible_id":        ids_tries[j],
                    "score_confiance": round(random.uniform(0.3, 0.7), 4),
                    "jours_offset":    random.randint(0, 28),
                })

    requete_adr = """
        UNWIND $paires AS p
        MATCH (a:Entite {identifiant: p.source_id})
        MATCH (b:Entite {identifiant: p.cible_id})
        MERGE (a)-[r:PARTAGE_ADRESSE]->(b)
        ON CREATE SET
            r.score_confiance = p.score_confiance,
            r.date_creation   = datetime() - duration({days: p.jours_offset}),
            r.inactif         = false
        RETURN count(r) AS nb
    """
    nb_adr = 0
    for i in range(0, len(paires_adr), taille_batch):
        res = connexion.executer_requete_ecriture(requete_adr, {"paires": paires_adr[i:i+taille_batch]})
        nb_adr += res[0]["nb"] if res else 0
    total += nb_adr
    journal.info(f"  PARTAGE_ADRESSE : {nb_adr} relations")

    # ── Relations COSIGNATURE (réseaux de fraude) ─────────────────
    journal.info("Création des relations COSIGNATURE (réseaux de fraude)...")
    paires_cosig = []
    for ids in groupes_reseau.values():
        ids_tries = sorted(ids)
        for i in range(len(ids_tries)):
            for j in range(i + 1, len(ids_tries)):
                paires_cosig.append({
                    "source_id":       ids_tries[i],
                    "cible_id":        ids_tries[j],
                    "score_confiance": round(random.uniform(0.1, 0.4), 4),
                    "jours_offset":    random.randint(0, 28),
                })

    requete_cosig = """
        UNWIND $paires AS p
        MATCH (a:Entite {identifiant: p.source_id})
        MATCH (b:Entite {identifiant: p.cible_id})
        MERGE (a)-[r:COSIGNATURE]->(b)
        ON CREATE SET
            r.score_confiance = p.score_confiance,
            r.date_creation   = datetime() - duration({days: p.jours_offset}),
            r.inactif         = false
        RETURN count(r) AS nb
    """
    nb_cosig = 0
    for i in range(0, len(paires_cosig), taille_batch):
        res = connexion.executer_requete_ecriture(requete_cosig, {"paires": paires_cosig[i:i+taille_batch]})
        nb_cosig += res[0]["nb"] if res else 0
    total += nb_cosig
    journal.info(f"  COSIGNATURE : {nb_cosig} relations")

    # ── Relations CAUSE_DE (transactions) ────────────────────────
    journal.info("Création des relations CAUSE_DE (transactions)...")
    ids_entites = [e["identifiant"] for e in entites]
    paires_cause: List[Dict] = []

    for entite in entites:
        est_suspect = entite["categorie"] in ("SUSPECT", "FRAUDE")
        nb_rel = random.randint(1, 5 if est_suspect else 3)
        cibles = random.sample(ids_entites, min(nb_rel, len(ids_entites) - 1))
        for cible_id in cibles:
            if cible_id == entite["identifiant"]:
                continue
            paires_cause.append({
                "source_id":       entite["identifiant"],
                "cible_id":        cible_id,
                "delta_secondes":  float(random.randint(60, 72 * 3600)),
                "score_confiance": round(random.uniform(0.1, 0.9), 4),
                "jours_offset":    random.randint(0, 28),
                "inactif":         random.random() < 0.05,
            })

    requete_cause = """
        UNWIND $paires AS p
        MATCH (a:Entite {identifiant: p.source_id})
        MATCH (b:Entite {identifiant: p.cible_id})
        MERGE (a)-[r:CAUSE_DE]->(b)
        ON CREATE SET
            r.delta_secondes  = p.delta_secondes,
            r.score_confiance = p.score_confiance,
            r.date_creation   = datetime() - duration({days: p.jours_offset}),
            r.inactif         = p.inactif
        RETURN count(r) AS nb
    """
    nb_cause = 0
    for i in range(0, len(paires_cause), taille_batch):
        res = connexion.executer_requete_ecriture(requete_cause, {"paires": paires_cause[i:i+taille_batch]})
        nb_cause += res[0]["nb"] if res else 0
        journal.info(f"  CAUSE_DE : {nb_cause}/{len(paires_cause)} relations")
    total += nb_cause

    journal.info(f"Relations créées au total : {total}")
    return total


# ================================================================
#  NETTOYAGE DES DONNÉES EXISTANTES
# ================================================================

def vider_base(connexion: ConnexionNeo4j) -> None:
    """
    Supprime tous les nœuds et relations existants.
    À utiliser avec précaution en production.
    """
    journal.warning("Suppression de toutes les données existantes...")
    # Supprimer par lots pour éviter les timeouts sur gros graphes
    while True:
        res = connexion.executer_requete_ecriture("""
            MATCH (n)
            WITH n LIMIT 5000
            DETACH DELETE n
            RETURN count(n) AS supprimes
        """)
        nb = res[0]["supprimes"] if res else 0
        journal.info(f"  Supprimés : {nb}")
        if nb == 0:
            break
    journal.info("Base vidée.")


# ================================================================
#  PIPELINE PRINCIPAL DE GÉNÉRATION
# ================================================================

def generer_et_inserer(
    connexion: ConnexionNeo4j,
    nb_entites: int = NB_ENTITES,
    vider_existant: bool = False,
    seed: Optional[int] = None
) -> Dict:
    """
    Pipeline complet de génération de données synthétiques.

    Paramètres :
        connexion     — connexion Neo4j active
        nb_entites    — nombre d'entités à générer (défaut : 1000)
        vider_existant — vider la base avant insertion (défaut : False)
        seed          — graine aléatoire pour reproductibilité (défaut : None = aléatoire)

    Retourne un résumé des données insérées.
    """
    if seed is not None:
        random.seed(seed)
        journal.info(f"Graine aléatoire fixée : {seed}")

    debut = datetime.now(timezone.utc)
    journal.info(f"Génération de {nb_entites} entités...")

    if vider_existant:
        vider_base(connexion)

    # Générer les entités
    entites  = generer_entites(nb_entites)
    # Générer les événements (pour la Phase 1 du pipeline)
    evenements = generer_evenements(entites)

    # Insérer dans Neo4j
    nb_entites_inserees = inserer_entites_neo4j(entites, connexion)
    nb_relations        = inserer_relations_neo4j(entites, connexion)

    duree = (datetime.now(timezone.utc) - debut).total_seconds()

    resume = {
        "entites_generees":    len(entites),
        "entites_inserees":    nb_entites_inserees,
        "relations_creees":    nb_relations,
        "evenements_generes":  len(evenements),
        "duree_secondes":      round(duree, 2),
        "normales":  sum(1 for e in entites if e["categorie"] == "NORMAL"),
        "suspectes": sum(1 for e in entites if e["categorie"] == "SUSPECT"),
        "fraude":    sum(1 for e in entites if e["categorie"] == "FRAUDE"),
    }
    journal.info(
        f"Génération terminée en {duree:.1f}s — "
        f"{nb_entites_inserees} entités, {nb_relations} relations"
    )
    return resume, evenements