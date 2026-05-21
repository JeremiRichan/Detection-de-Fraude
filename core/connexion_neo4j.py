# ================================================================
#  CONNEXION NEO4J ET UTILITAIRES PARTAGÉS
#  Gestion de la connexion, sessions, transactions
#
#  CORRECTIONS APPLIQUÉES :
#  - Suppression de sys.path.append (chemin d'import cassé)
#  - Singleton thread-safe avec threading.Lock (double-checked locking)
#  - Rollback explicite dans executer_transaction_multiple
#  - json.dumps pour sérialiser les données brutes (str() invalide)
# ================================================================

import hashlib
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from neo4j import GraphDatabase, Session, Transaction
from neo4j.exceptions import ServiceUnavailable, AuthError

# CORRECTION : import direct sans sys.path.append
from config.configuration import (
    URI_NEO4J,
    UTILISATEUR_NEO4J,
    MOT_DE_PASSE_NEO4J,
    NOM_BASE_NEO4J
)

# ── Configuration du journal d'événements ───────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
journal_connexion = logging.getLogger("ConnexionNeo4j")

# Supprimer les notifications Neo4j de niveau INFO/WARNING (cartesian product, labels manquants)
# Ces avertissements sont attendus sur une base vide ou en développement
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)


# ================================================================
#  CLASSE PRINCIPALE DE CONNEXION
# ================================================================

class ConnexionNeo4j:
    """
    Gestionnaire de connexion Neo4j avec gestion des sessions,
    transactions et retry automatique.
    """

    def __init__(self):
        self._pilote = None
        self._uri           = URI_NEO4J
        self._utilisateur   = UTILISATEUR_NEO4J
        self._mot_de_passe  = MOT_DE_PASSE_NEO4J
        self._nom_base      = NOM_BASE_NEO4J
        self._connecter()

    def _connecter(self) -> None:
        """Établit la connexion au serveur Neo4j."""
        try:
            self._pilote = GraphDatabase.driver(
                self._uri,
                auth=(self._utilisateur, self._mot_de_passe),
                max_connection_lifetime=3600,
                max_connection_pool_size=50,
                connection_acquisition_timeout=60
            )
            self._pilote.verify_connectivity()
            journal_connexion.info(f"Connexion Neo4j établie : {self._uri}")
        except AuthError as erreur_auth:
            journal_connexion.error(f"Erreur d'authentification Neo4j : {erreur_auth}")
            raise
        except ServiceUnavailable as erreur_service:
            journal_connexion.error(f"Serveur Neo4j indisponible : {erreur_service}")
            raise

    def obtenir_session(self) -> Session:
        """Retourne une session Neo4j sur la base de données configurée."""
        return self._pilote.session(database=self._nom_base)

    def executer_requete_lecture(
        self,
        requete_cypher: str,
        parametres: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Exécute une requête de lecture Cypher.
        Retourne la liste des enregistrements sous forme de dictionnaires.
        """
        parametres = parametres or {}
        with self.obtenir_session() as session:
            resultat = session.run(requete_cypher, parametres)
            return [dict(enregistrement) for enregistrement in resultat]

    def executer_requete_ecriture(
        self,
        requete_cypher: str,
        parametres: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Exécute une requête d'écriture Cypher dans une transaction.
        Garantit l'atomicité de l'opération.
        """
        parametres = parametres or {}
        with self.obtenir_session() as session:
            resultat = session.execute_write(
                lambda transaction: list(
                    transaction.run(requete_cypher, parametres)
                )
            )
            return [dict(enregistrement) for enregistrement in resultat]

    def executer_transaction_multiple(
        self,
        liste_requetes: List[Tuple[str, Dict]]
    ) -> bool:
        """
        Exécute plusieurs requêtes dans une seule transaction atomique.
        Si une requête échoue, toutes sont annulées (rollback explicite).

        CORRECTION : rollback explicite ajouté pour garantir la cohérence.
        """
        with self.obtenir_session() as session:
            transaction = session.begin_transaction()
            try:
                for requete_cypher, parametres in liste_requetes:
                    transaction.run(requete_cypher, parametres)
                transaction.commit()
                return True
            except Exception as erreur:
                journal_connexion.error(
                    f"Transaction annulée (rollback) : {erreur}"
                )
                # CORRECTION : rollback explicite
                try:
                    transaction.rollback()
                except Exception as erreur_rollback:
                    journal_connexion.error(
                        f"Erreur lors du rollback : {erreur_rollback}"
                    )
                return False

    def fermer(self) -> None:
        """Ferme proprement la connexion au serveur Neo4j."""
        if self._pilote:
            self._pilote.close()
            journal_connexion.info("Connexion Neo4j fermée.")

    def __enter__(self):
        return self

    def __exit__(self, type_exception, valeur_exception, traceback):
        self.fermer()


# ================================================================
#  UTILITAIRES PARTAGÉS
# ================================================================

def calculer_empreinte_sha256(contenu: str) -> str:
    """
    Calcule l'empreinte SHA-256 d'un contenu pour garantir
    l'immuabilité des enregistrements de conformité (RGPD Art. 22).
    """
    return hashlib.sha256(contenu.encode('utf-8')).hexdigest()


def horodatage_actuel_utc() -> datetime:
    """Retourne l'horodatage actuel en UTC."""
    return datetime.now(timezone.utc)


def normaliser_identifiant(identifiant_brut: str) -> str:
    """
    Normalise un identifiant d'entité :
    minuscules, suppression des espaces superflus, encodage UTF-8.
    """
    if identifiant_brut is None:
        return ""
    return identifiant_brut.strip().lower()


def convertir_horodatage_utc(horodatage_brut) -> datetime:
    """
    Convertit un horodatage quelconque en UTC.
    Gère les formats ISO 8601, Unix timestamp et datetime natifs.
    """
    if isinstance(horodatage_brut, datetime):
        if horodatage_brut.tzinfo is None:
            return horodatage_brut.replace(tzinfo=timezone.utc)
        return horodatage_brut.astimezone(timezone.utc)
    if isinstance(horodatage_brut, (int, float)):
        return datetime.fromtimestamp(horodatage_brut, tz=timezone.utc)
    if isinstance(horodatage_brut, str):
        return datetime.fromisoformat(horodatage_brut).astimezone(timezone.utc)
    raise ValueError(f"Format d'horodatage non reconnu : {type(horodatage_brut)}")


def enregistrer_rejet(
    connexion_neo4j: ConnexionNeo4j,
    evenement_brut: Dict,
    motif_rejet: str
) -> None:
    """
    Enregistre un événement rejeté dans un journal immuable Neo4j.
    Utilisé par les algorithmes V1 à V4 de la Phase 1.

    CORRECTION : json.dumps remplace str() pour une sérialisation valide.
    """
    requete = """
        CREATE (rejet:EvenementRejete {
            identifiant_rejet:  randomUUID(),
            identifiant_source: $identifiant_source,
            type_evenement:     $type_evenement,
            motif_rejet:        $motif_rejet,
            horodatage_rejet:   datetime(),
            donnees_brutes:     $donnees_brutes
        })
    """
    # CORRECTION : json.dumps produit du JSON valide (str() produisait du Python)
    donnees_serialisees = json.dumps(
        evenement_brut,
        default=str,
        ensure_ascii=False
    )[:500]

    parametres = {
        "identifiant_source": evenement_brut.get("id_source", "INCONNU"),
        "type_evenement":     evenement_brut.get("type_evenement", "INCONNU"),
        "motif_rejet":        motif_rejet,
        "donnees_brutes":     donnees_serialisees
    }
    connexion_neo4j.executer_requete_ecriture(requete, parametres)


def marquer_evenement(evenement_brut: Dict, flag: str) -> Dict:
    """
    Ajoute un flag sémantique à un événement brut.
    L'événement continue son traitement mais sera pondéré différemment.
    """
    if "flags_semantiques" not in evenement_brut:
        evenement_brut["flags_semantiques"] = []
    evenement_brut["flags_semantiques"].append(flag)
    return evenement_brut


# ================================================================
#  SINGLETON THREAD-SAFE
#  CORRECTION : double-checked locking avec threading.Lock
# ================================================================

_instance_connexion: Optional[ConnexionNeo4j] = None
_verrou_singleton = threading.Lock()


def obtenir_connexion() -> ConnexionNeo4j:
    """
    Retourne l'instance singleton de connexion Neo4j.
    Thread-safe via double-checked locking.
    """
    global _instance_connexion
    if _instance_connexion is None:
        with _verrou_singleton:
            if _instance_connexion is None:
                _instance_connexion = ConnexionNeo4j()
    return _instance_connexion