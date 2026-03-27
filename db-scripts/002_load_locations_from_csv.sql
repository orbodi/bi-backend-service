-- Charger un CSV centres -> bi.location (hiérarchie) + bi.locations (centres)
-- Prérequis: exécuter 001_bi_schema_locations.sql
-- Usage psql:
--   \i bi-backend-service/db-scripts/002_load_locations_from_csv.sql
--   \copy bi.stg_locations_csv(region, code_region, prefecture, commune, canton, localite, localites, locationcode, centres, code_centres)
--   from 'C:/path/centres.csv' with (format csv, header true, encoding 'UTF8');
--   select bi.load_locations_from_staging('fra');

CREATE TABLE IF NOT EXISTS bi.stg_locations_csv (
  region        TEXT,
  code_region   TEXT,
  prefecture    TEXT,
  commune       TEXT,
  canton        TEXT,
  localite      TEXT,
  localites     TEXT,
  locationcode  TEXT,
  centres       TEXT,
  code_centres  TEXT
);

CREATE OR REPLACE FUNCTION bi.load_locations_from_staging(p_lang_code TEXT DEFAULT 'fra')
RETURNS TABLE(inserted_locations BIGINT, upserted_centers BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
  v_loc_count BIGINT := 0;
  v_center_count BIGINT := 0;
BEGIN
  IF p_lang_code IS NULL OR length(trim(p_lang_code)) = 0 THEN
    RAISE EXCEPTION 'p_lang_code est requis';
  END IF;

  -- 0) Pays racine (root)
  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  VALUES ('TG', 'TOGO', 0, 'Country', NULL, p_lang_code, TRUE, 'csv_loader', FALSE)
  ON CONFLICT (code, lang_code) DO NOTHING;

  -- Temp tables : nettoyage pour permettre un re-run de la fonction
  DROP TABLE IF EXISTS tmp_src;
  DROP TABLE IF EXISTS tmp_region_lkp;
  DROP TABLE IF EXISTS tmp_prefecture_map;
  DROP TABLE IF EXISTS tmp_commune_map;
  DROP TABLE IF EXISTS tmp_canton_map;
  DROP TABLE IF EXISTS tmp_locality_map;
  DROP TABLE IF EXISTS tmp_center_lkp;

  -- src (staging filtré) dans une table temporaire pour réutilisation
  CREATE TEMP TABLE tmp_src ON COMMIT DROP AS
  SELECT
    trim(region)       AS region,
    trim(code_region)  AS code_region,
    trim(prefecture)   AS prefecture,
    trim(commune)      AS commune,
    trim(canton)       AS canton,
    trim(localite)     AS localite,
    trim(locationcode) AS locationcode,
    trim(centres)      AS centres,
    trim(code_centres) AS code_centres
  FROM bi.stg_locations_csv
  WHERE coalesce(trim(region), '') <> ''
    AND coalesce(trim(code_region), '') <> ''
    AND coalesce(trim(prefecture), '') <> ''
    AND coalesce(trim(commune), '') <> ''
    AND coalesce(trim(canton), '') <> ''
    AND coalesce(trim(localite), '') <> ''
    AND coalesce(trim(locationcode), '') <> ''
    AND coalesce(trim(centres), '') <> ''
    AND coalesce(trim(code_centres), '') <> '';

  -- 1) Regions
  CREATE TEMP TABLE tmp_region_lkp ON COMMIT DROP AS
  SELECT DISTINCT code_region, region
  FROM tmp_src;

  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  SELECT
    r.code_region,
    r.region,
    1::smallint,
    'Region',
    'TG'::text,
    p_lang_code,
    TRUE,
    'csv_loader',
    FALSE
  FROM tmp_region_lkp r
  ON CONFLICT (code, lang_code)
  DO UPDATE SET
    name = EXCLUDED.name,
    parent_loc_code = EXCLUDED.parent_loc_code,
    hierarchy_level_name = EXCLUDED.hierarchy_level_name,
    upd_by = 'csv_loader',
    upd_dtimes = CURRENT_TIMESTAMP;

  -- 2) Préfectures (distinctes)
  CREATE TEMP TABLE tmp_prefecture_map ON COMMIT DROP AS
  SELECT
    t.code_region,
    t.prefecture,
    ('P' || row_number() OVER (ORDER BY t.code_region, t.prefecture))::TEXT AS code,
    t.code_region AS parent_code
  FROM (
    SELECT DISTINCT code_region, prefecture
    FROM tmp_src
  ) t;

  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  SELECT
    p.code,
    p.prefecture,
    2::smallint,
    'prefecture',
    p.parent_code,
    p_lang_code,
    TRUE,
    'csv_loader',
    FALSE
  FROM tmp_prefecture_map p
  ON CONFLICT (code, lang_code)
  DO UPDATE SET
    name = EXCLUDED.name,
    parent_loc_code = EXCLUDED.parent_loc_code,
    hierarchy_level_name = EXCLUDED.hierarchy_level_name,
    upd_by = 'csv_loader',
    upd_dtimes = CURRENT_TIMESTAMP;

  -- 3) Communes (distinctes)
  CREATE TEMP TABLE tmp_commune_map ON COMMIT DROP AS
  SELECT
    c.code_region,
    c.prefecture,
    c.commune,
    pm.code AS parent_code,
    ('COM' || row_number() OVER (ORDER BY c.code_region, c.prefecture, c.commune))::TEXT AS code
  FROM (
    SELECT DISTINCT code_region, prefecture, commune
    FROM tmp_src
  ) c
  JOIN tmp_prefecture_map pm
    ON pm.code_region = c.code_region
   AND pm.prefecture = c.prefecture;

  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  SELECT
    cm.code,
    cm.commune,
    3::smallint,
    'commun',
    cm.parent_code,
    p_lang_code,
    TRUE,
    'csv_loader',
    FALSE
  FROM tmp_commune_map cm
  ON CONFLICT (code, lang_code)
  DO UPDATE SET
    name = EXCLUDED.name,
    parent_loc_code = EXCLUDED.parent_loc_code,
    hierarchy_level_name = EXCLUDED.hierarchy_level_name,
    upd_by = 'csv_loader',
    upd_dtimes = CURRENT_TIMESTAMP;

  -- 4) Cantons (distinctes)
  CREATE TEMP TABLE tmp_canton_map ON COMMIT DROP AS
  SELECT
    c.code_region,
    c.prefecture,
    c.commune,
    c.canton,
    cm.code AS parent_code,
    ('CTN' || row_number() OVER (ORDER BY c.code_region, c.prefecture, c.commune, c.canton))::TEXT AS code
  FROM (
    SELECT DISTINCT code_region, prefecture, commune, canton
    FROM tmp_src
  ) c
  JOIN tmp_commune_map cm
    ON cm.code_region = c.code_region
   AND cm.prefecture = c.prefecture
   AND cm.commune = c.commune;

  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  SELECT
    cn.code,
    cn.canton,
    4::smallint,
    'canton',
    cn.parent_code,
    p_lang_code,
    TRUE,
    'csv_loader',
    FALSE
  FROM tmp_canton_map cn
  ON CONFLICT (code, lang_code)
  DO UPDATE SET
    name = EXCLUDED.name,
    parent_loc_code = EXCLUDED.parent_loc_code,
    hierarchy_level_name = EXCLUDED.hierarchy_level_name,
    upd_by = 'csv_loader',
    upd_dtimes = CURRENT_TIMESTAMP;

  -- 5) Localités (code=locationcode, parent=canton)
  CREATE TEMP TABLE tmp_locality_map ON COMMIT DROP AS
  SELECT DISTINCT
    s.locationcode AS code,
    s.localite AS name,
    cn.code AS parent_code
  FROM tmp_src s
  JOIN tmp_canton_map cn
    ON cn.code_region = s.code_region
   AND cn.prefecture = s.prefecture
   AND cn.commune = s.commune
   AND cn.canton = s.canton;

  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  SELECT
    l.code,
    l.name,
    5::smallint,
    'locality',
    l.parent_code,
    p_lang_code,
    TRUE,
    'csv_loader',
    FALSE
  FROM tmp_locality_map l
  ON CONFLICT (code, lang_code)
  DO UPDATE SET
    name = EXCLUDED.name,
    parent_loc_code = EXCLUDED.parent_loc_code,
    hierarchy_level_name = EXCLUDED.hierarchy_level_name,
    upd_by = 'csv_loader',
    upd_dtimes = CURRENT_TIMESTAMP;

  -- 6) Centres (code=code_centres, parent=locality=locationcode)
  CREATE TEMP TABLE tmp_center_lkp ON COMMIT DROP AS
  SELECT DISTINCT
    s.code_centres,
    s.centres,
    s.locationcode,
    s.code_region,
    s.region,
    s.prefecture,
    s.commune,
    s.canton,
    s.localite
  FROM tmp_src s;

  INSERT INTO bi.location (
    code, name, hierarchy_level, hierarchy_level_name, parent_loc_code,
    lang_code, is_active, cr_by, is_deleted
  )
  SELECT
    c.code_centres,
    c.centres,
    6::smallint,
    'center',
    c.locationcode,
    p_lang_code,
    TRUE,
    'csv_loader',
    FALSE
  FROM tmp_center_lkp c
  ON CONFLICT (code, lang_code)
  DO UPDATE SET
    name = EXCLUDED.name,
    parent_loc_code = EXCLUDED.parent_loc_code,
    hierarchy_level_name = EXCLUDED.hierarchy_level_name,
    upd_by = 'csv_loader',
    upd_dtimes = CURRENT_TIMESTAMP;

  -- Centres dans bi.locations (table BI consommation)
  INSERT INTO bi.locations (
    center_code, center_name, locality_code, lang_code,
    region_code, prefecture_code, commune_code, canton_code,
    region_name, prefecture_name, commune_name, canton_name, locality_name,
    is_active, updated_at
  )
  SELECT DISTINCT
    c.code_centres AS center_code,
    c.centres AS center_name,
    c.locationcode AS locality_code,
    p_lang_code AS lang_code,
    c.code_region AS region_code,
    pm.code AS prefecture_code,
    cm.code AS commune_code,
    cn.code AS canton_code,
    c.region AS region_name,
    c.prefecture AS prefecture_name,
    c.commune AS commune_name,
    c.canton AS canton_name,
    c.localite AS locality_name,
    TRUE,
    CURRENT_TIMESTAMP AS updated_at
  FROM tmp_center_lkp c
  JOIN tmp_prefecture_map pm
    ON pm.code_region = c.code_region
   AND pm.prefecture = c.prefecture
  JOIN tmp_commune_map cm
    ON cm.code_region = c.code_region
   AND cm.prefecture = c.prefecture
   AND cm.commune = c.commune
  JOIN tmp_canton_map cn
    ON cn.code_region = c.code_region
   AND cn.prefecture = c.prefecture
   AND cn.commune = c.commune
   AND cn.canton = c.canton
  ON CONFLICT (center_code)
  DO UPDATE SET
    center_name = EXCLUDED.center_name,
    locality_code = EXCLUDED.locality_code,
    lang_code = EXCLUDED.lang_code,
    region_code = EXCLUDED.region_code,
    prefecture_code = EXCLUDED.prefecture_code,
    commune_code = EXCLUDED.commune_code,
    canton_code = EXCLUDED.canton_code,
    region_name = EXCLUDED.region_name,
    prefecture_name = EXCLUDED.prefecture_name,
    commune_name = EXCLUDED.commune_name,
    canton_name = EXCLUDED.canton_name,
    locality_name = EXCLUDED.locality_name,
    is_active = EXCLUDED.is_active,
    updated_at = CURRENT_TIMESTAMP;

  -- Comptage (distinct des temp tables)
  SELECT
    (SELECT count(*) FROM tmp_region_lkp)
    + (SELECT count(*) FROM tmp_prefecture_map)
    + (SELECT count(*) FROM tmp_commune_map)
    + (SELECT count(*) FROM tmp_canton_map)
    + (SELECT count(*) FROM tmp_locality_map)
    + (SELECT count(*) FROM tmp_center_lkp)
  INTO v_loc_count;

  SELECT count(*) INTO v_center_count FROM tmp_center_lkp;

  RETURN QUERY SELECT v_loc_count, v_center_count;
END;
$$;
