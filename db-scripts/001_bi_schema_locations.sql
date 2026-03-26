-- Schéma BI : tables de dimensions (géographie alignée MOSIP) et, plus tard, vues de reporting.
-- Exécuter sur PostgreSQL (psql, DBeaver, etc.)
--
-- FK parent : DEFERRABLE INITIALLY DEFERRED permet, dans une même transaction,
-- d’insérer des lignes dans un ordre quelconque tant que l’arbre est cohérent au COMMIT.

CREATE SCHEMA IF NOT EXISTS bi;

COMMENT ON SCHEMA bi IS 'Cible BI : dimensions, faits agrégés et vues de reporting.';

-- ---------------------------------------------------------------------------
-- Dictionnaire des niveaux (0 = pays … 5 = localité), une ligne par langue
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bi.loc_hierarchy_list (
  hierarchy_level       SMALLINT NOT NULL,
  hierarchy_level_name  VARCHAR(128) NOT NULL,
  lang_code             VARCHAR(3) NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  cr_by                 VARCHAR(256),
  cr_dtimes             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
  upd_by                VARCHAR(256),
  upd_dtimes            TIMESTAMP(6),
  is_deleted            BOOLEAN NOT NULL DEFAULT FALSE,
  del_dtimes            TIMESTAMP(6),
  CONSTRAINT pk_loc_hierarchy_list PRIMARY KEY (hierarchy_level, lang_code)
);

CREATE INDEX IF NOT EXISTS ix_loc_hierarchy_list_lang
  ON bi.loc_hierarchy_list (lang_code);

-- ---------------------------------------------------------------------------
-- Arbre géographique : même table pour tous les niveaux, parent dans la table
-- PK (code, lang_code) : même code métier en fra / eng = deux lignes
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bi.location (
  code                  VARCHAR(64) NOT NULL,
  name                  VARCHAR(512) NOT NULL,
  hierarchy_level       SMALLINT NOT NULL,
  hierarchy_level_name  VARCHAR(128) NOT NULL,
  parent_loc_code       VARCHAR(64),
  lang_code             VARCHAR(3) NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  cr_by                 VARCHAR(256),
  cr_dtimes             TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
  upd_by                VARCHAR(256),
  upd_dtimes            TIMESTAMP(6),
  is_deleted            BOOLEAN NOT NULL DEFAULT FALSE,
  del_dtimes            TIMESTAMP(6),
  CONSTRAINT pk_location PRIMARY KEY (code, lang_code),
  CONSTRAINT fk_location_parent
    FOREIGN KEY (parent_loc_code, lang_code)
    REFERENCES bi.location (code, lang_code)
    DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT fk_location_hierarchy_meta
    FOREIGN KEY (hierarchy_level, lang_code)
    REFERENCES bi.loc_hierarchy_list (hierarchy_level, lang_code)
);

CREATE INDEX IF NOT EXISTS ix_location_parent
  ON bi.location (parent_loc_code, lang_code);

CREATE INDEX IF NOT EXISTS ix_location_level_lang
  ON bi.location (hierarchy_level, lang_code);

CREATE INDEX IF NOT EXISTS ix_location_active_lang
  ON bi.location (lang_code)
  WHERE NOT is_deleted;

-- ---------------------------------------------------------------------------
-- Centres d'enrôlement (table consommation BI)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bi.locations (
  center_code           VARCHAR(64) PRIMARY KEY,
  center_code_short     VARCHAR(32),
  center_name           VARCHAR(512) NOT NULL,
  location_code         VARCHAR(64) NOT NULL,
  lang_code             VARCHAR(3) NOT NULL DEFAULT 'fra',
  region_code           VARCHAR(32),
  region_name           VARCHAR(128),
  prefecture_name       VARCHAR(128),
  commune_name          VARCHAR(128),
  canton_name           VARCHAR(128),
  locality_name         VARCHAR(128),
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at            TIMESTAMP(6),
  CONSTRAINT fk_locations_locality
    FOREIGN KEY (location_code, lang_code)
    REFERENCES bi.location (code, lang_code)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS ix_locations_region
  ON bi.locations (region_name);

CREATE INDEX IF NOT EXISTS ix_locations_prefecture
  ON bi.locations (prefecture_name);

CREATE INDEX IF NOT EXISTS ix_locations_location_code
  ON bi.locations (location_code);

-- ---------------------------------------------------------------------------
-- Données de référence des niveaux (aligné MOSIP master.loc_hierarchy_list)
-- ---------------------------------------------------------------------------
INSERT INTO bi.loc_hierarchy_list
  (hierarchy_level, hierarchy_level_name, lang_code, is_active, cr_by, cr_dtimes, is_deleted)
VALUES
  (0, 'Country', 'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (0, 'Pays',    'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (1, 'Region',  'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (1, 'Region',  'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (2, 'prefecture', 'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (2, 'prefecture', 'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (3, 'commun',  'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (3, 'commun',  'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (4, 'canton',  'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (4, 'canton',  'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (5, 'locality', 'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (5, 'locality', 'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (6, 'center',   'eng', TRUE, 'system', CURRENT_TIMESTAMP, FALSE),
  (6, 'center',   'fra', TRUE, 'system', CURRENT_TIMESTAMP, FALSE)
ON CONFLICT (hierarchy_level, lang_code) DO NOTHING;
