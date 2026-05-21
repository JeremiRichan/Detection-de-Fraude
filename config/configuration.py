
# ── Connexion Neo4j ──────────────────────────────────────────────
URI_NEO4J                           = "neo4j://127.0.0.1:7687"
UTILISATEUR_NEO4J                   = "neo4j"
MOT_DE_PASSE_NEO4J                  = "fraude123"
NOM_BASE_NEO4J                      = "neo4j"

# ── Phase 1 — Ingestion ──────────────────────────────────────────
TYPES_EVENEMENTS_AUTORISES          = {
    "TRANSACTION", "EMAIL", "SMS",
    "LOG_ACCES", "APPROBATION",
    "PARTAGE_IP", "COSIGNATURE", "PARTAGE_ADRESSE"
}
TYPES_SOURCE_AUTORISES              = {"INTERNE", "EXTERNE"}
DUREE_FENETRE_DEDUPLICATION_SEC     = 3600
FACTEUR_SPIKE_VOLUME                = 10.0
DUREE_FENETRE_SPIKE_MINUTES         = 10
DUREE_RETRODATE_MAXIMUM_JOURS       = 30
SEUIL_CORROBORATION_MINIMUM         = 0.33
NOMBRE_SOURCES_MAXIMUM              = 5
FACTEUR_ABERRANCE_MONTANT           = 5.0
SAUT_HIERARCHIQUE_MAXIMUM           = 2

# ── Phase 2 — Résolution d'entités ──────────────────────────────

SEUIL_FUSION_ENTITES                = 0.80
DISTANCE_MAX_PREFILTRAGE            = 0.30
POIDS_SIMILARITE_NOM                = 0.15
POIDS_SIMILARITE_PHONETIQUE         = 0.08
POIDS_SIMILARITE_ADRESSE            = 0.12
POIDS_SIMILARITE_TELEPHONE          = 0.15
POIDS_SIMILARITE_EMAIL              = 0.15
POIDS_SIMILARITE_IP                 = 0.12
POIDS_SIMILARITE_COMPTE             = 0.13
POIDS_SIMILARITE_SIRET              = 0.10
JOURS_IDENTITE_SYNTHETIQUE          = 30
NOMBRE_ENTITES_RECENTES_MIN         = 3
ATTRIBUTS_COMMUNS_MIN               = 2

# ── Phase 3 — Graphe ─────────────────────────────────────────────
DUREE_FENETRE_CAUSALITE_HEURES      = 72
DUREE_INACTIVITE_LIEN_JOURS         = 365
PATRONS_CAUSAUX_FRAUDE              = [
    ("EMAIL",        "TRANSACTION"),
    ("TRANSACTION",  "TRANSACTION"),
    ("LOG_ACCES",    "TRANSACTION"),
    ("EMAIL",        "APPROBATION"),
]

# ── Phase 4 — Topologie ──────────────────────────────────────────
FACTEUR_AMORTISSEMENT_PAGERANK      = 0.85
ITERATIONS_MAX_PAGERANK             = 100
TOLERANCE_CONVERGENCE_PAGERANK      = 1e-6
TAILLE_CLIQUE_MINIMUM               = 3
SEUIL_MULTIPLICATEUR_HUB            = 3.0

# ── Phase 5 — Confiance ──────────────────────────────────────────

POIDS_FREQUENCE                     = 0.25
POIDS_RECENCE                       = 0.20
POIDS_DIVERSITE_PREUVES             = 0.20
POIDS_JACCARD_VOISINS               = 0.20
POIDS_CORROBORATION                 = 0.15
TAUX_DECROISSANCE_RECENCE           = 0.005
FACTEUR_REDUCTION_LISTE_BLANCHE     = 0.30
NOMBRE_TYPES_PREUVES_MAX            = 5
ITERATIONS_MODELE_NUL               = 1000
SEUIL_SCORE_Z_SIGNIFICATIVITE       = 2.0
SEUIL_ASYMETRIE_FORTE               = 0.50

# ── Phase 6 — Communautés ────────────────────────────────────────
SEUIL_CHEVAUCHEMENT_CONTINUITE      = 0.70
SEUIL_CHEVAUCHEMENT_TRANSITION      = 0.30
SEUIL_FRAGMENTATION_SUSPECTE        = 5
SEUIL_COHESION_DIFFUSE              = 0.05
SEUIL_COHESION_NORMALE              = 0.30
SEUIL_COHESION_DENSE                = 0.60
SEUIL_DENSITE_NOYAU_DUR             = 0.70
AGE_MOYEN_NOYAU_DUR_JOURS          = 30
SEUIL_CONFIANCE_INTER_FUSION        = 0.10

# ── Phase 7 — Scoring ────────────────────────────────────────────
NOMBRE_ARBRES_ISOLATION_FOREST      = 100
TAILLE_ECHANTILLON_ISOLATION        = 256
POIDS_ISOLATION_FOREST              = 0.40
POIDS_GNN                           = 0.60
COUT_FAUX_NEGATIF                   = 50000.0
COUT_FAUX_POSITIF                   = 200.0
DIMENSION_VECTEUR_CARACTERISTIQUES  = 16

# ── Phase 8 — Évaluation ─────────────────────────────────────────
PROPORTION_ENTRAINEMENT             = 0.70
NOMBRE_CARACTERISTIQUES_DERIVE      = 3
SEUIL_REENTRAINEMENT_LABELS         = 50
INCREMENT_RISQUE_VOISIN             = 0.10
INCREMENT_RISQUE_COMMUNAUTE         = 0.05
DECREMENT_FAUX_POSITIF              = 0.15
SEUIL_COHESION_PROPAGATION          = 0.50
SEUIL_DISPARITE_BIAIS_MAX           = 1.50