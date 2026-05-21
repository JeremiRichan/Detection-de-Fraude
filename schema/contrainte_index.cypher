
// Entités : identifiant unique obligatoire
CREATE CONSTRAINT contrainte_entite_identifiant IF NOT EXISTS
FOR (e:Entite)
REQUIRE e.identifiant IS UNIQUE;

// Événements : identifiant source unique
CREATE CONSTRAINT contrainte_evenement_source IF NOT EXISTS
FOR (e:Evenement)
REQUIRE e.identifiant_source IS UNIQUE;

// Événements rejetés : identifiant de rejet unique
CREATE CONSTRAINT contrainte_rejet_identifiant IF NOT EXISTS
FOR (r:EvenementRejete)
REQUIRE r.identifiant_rejet IS UNIQUE;

// ── Index de performance ─────────────────────────────────────────

// Index sur le type d'événement (filtrage Phase 1)
CREATE INDEX index_evenement_type IF NOT EXISTS
FOR (e:Evenement)
ON (e.type_evenement);

// Index sur l'horodatage (requêtes temporelles Phases 3, 5)
CREATE INDEX index_evenement_horodatage IF NOT EXISTS
FOR (e:Evenement)
ON (e.horodatage);

// Index sur la communauté (requêtes Phase 6)
CREATE INDEX index_entite_communaute IF NOT EXISTS
FOR (e:Entite)
ON (e.id_communaute);

// Index sur le score de risque (requêtes Phase 7)
CREATE INDEX index_entite_score_risque IF NOT EXISTS
FOR (e:Entite)
ON (e.score_risque);

// Index sur la décision de fraude (reporting Phase 8)
CREATE INDEX index_entite_decision IF NOT EXISTS
FOR (e:Entite)
ON (e.decision_fraude);

// Index sur le label de vérité (entraînement Phase 8)
CREATE INDEX index_entite_label IF NOT EXISTS
FOR (e:Entite)
ON (e.label_verite);

// Index sur la date de création (requêtes temporelles Phase 2)
CREATE INDEX index_entite_creation IF NOT EXISTS
FOR (e:Entite)
ON (e.date_creation);

// Index composé : score_risque + decision (requêtes de reporting)
CREATE INDEX index_entite_risque_decision IF NOT EXISTS
FOR (e:Entite)
ON (e.score_risque, e.decision_fraude);

// ── Index sur les propriétés de liaison ─────────────────────────

// Index sur score_confiance des relations (Phase 5)
CREATE INDEX index_lien_confiance IF NOT EXISTS
FOR ()-[l:CAUSE_DE]-()
ON (l.score_confiance);

// Index sur l'état inactif des liens (Phase 3)
CREATE INDEX index_lien_inactif IF NOT EXISTS
FOR ()-[l:CAUSE_DE]-()
ON (l.inactif);

// ── Contraintes d'existence (propriétés obligatoires) ────────────

// Entité : identifiant requis
CREATE CONSTRAINT contrainte_entite_identifiant_requis IF NOT EXISTS
FOR (e:Entite)
REQUIRE e.identifiant IS NOT NULL;

// Événement : type requis
CREATE CONSTRAINT contrainte_evenement_type_requis IF NOT EXISTS
FOR (e:Evenement)
REQUIRE e.type_evenement IS NOT NULL;
