-- Cube KPI IDPS — alimentation type dashboard / API REST.
--
-- Contexte batch :
--   Fichiers CSV reçus chaque jour vers 02h00 avec l’activité de la veille,
--   puis persistance en base (idps.workflow_events, idps.error_events, …).
--
-- Convention « jour d’activité » (activity_date) :
--   - Commandes MosIP : date du premier événement workflow connu pour la requête
--     (date(min(event_timestamp)) par request_id).
--   - Erreurs QC / SUP / PERSO : date(error_events.event_timestamp) tronquée au jour
--     (fuseau = session PostgreSQL, comme 005/006).
--   - Impressions en attente : effectif fin de journée « as-of » issu du cube 005/006
--     (bi.mv_idps_print_orders_daily_center_status, status_final = PENDING).
--
--   Si l’ETL ajoute une colonne métier explicite (ex. reporting_date), remplacer
--   les expressions date(...) par cette colonne pour coller au périmètre fichier.
--
-- Prérequis : 005 ou 006 appliqués (MV bi.mv_idps_print_orders_daily_center_status).
--
-- Refresh recommandé (après chargement CSV + refresh du cube statuts) :
--   REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_orders_daily_center_status;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_daily_kpis_mosip;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_daily_kpis_mosip_by_center;

CREATE SCHEMA IF NOT EXISTS bi;

-- ---------------------------------------------------------------------------
-- 1) Cube global : 1 ligne / activity_date
--    KPI : ordres MosIP, attentes, erreurs QC, SUP, PERSO (personnalisation)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS bi.v_idps_daily_kpis_mosip_geo;

DROP MATERIALIZED VIEW IF EXISTS bi.mv_idps_daily_kpis_mosip_by_center;
DROP MATERIALIZED VIEW IF EXISTS bi.mv_idps_daily_kpis_mosip;

CREATE MATERIALIZED VIEW bi.mv_idps_daily_kpis_mosip AS
WITH received AS (
  SELECT
    f.activity_date,
    count(*)::bigint AS orders_received_from_mosip
  FROM (
    SELECT
      w.request_id,
      date(min(w.event_timestamp)) AS activity_date
    FROM idps.workflow_events w
    GROUP BY w.request_id
  ) f
  GROUP BY f.activity_date
),
pending AS (
  SELECT
    m.kpi_date AS activity_date,
    sum(m.request_count)::bigint AS pending_impressions
  FROM bi.mv_idps_print_orders_daily_center_status m
  WHERE m.status_final = 'PENDING'
  GROUP BY m.kpi_date
),
err AS (
  SELECT
    date(e.event_timestamp) AS activity_date,
    count(*) FILTER (WHERE e.error_category = 'QC_ERROR')::bigint AS errors_qc,
    count(*) FILTER (WHERE e.error_category = 'SUP_ERROR')::bigint AS errors_sup,
    count(*) FILTER (WHERE e.error_category = 'PERSO_ERROR')::bigint AS errors_perso
  FROM idps.error_events e
  GROUP BY date(e.event_timestamp)
),
days AS (
  SELECT activity_date FROM received
  UNION
  SELECT activity_date FROM pending
  UNION
  SELECT activity_date FROM err
)
SELECT
  d.activity_date,
  coalesce(r.orders_received_from_mosip, 0)::bigint AS orders_received_from_mosip,
  coalesce(p.pending_impressions, 0)::bigint AS pending_impressions,
  coalesce(e.errors_qc, 0)::bigint AS errors_qc,
  coalesce(e.errors_sup, 0)::bigint AS errors_sup,
  coalesce(e.errors_perso, 0)::bigint AS errors_perso
FROM days d
LEFT JOIN received r ON r.activity_date = d.activity_date
LEFT JOIN pending p ON p.activity_date = d.activity_date
LEFT JOIN err e ON e.activity_date = d.activity_date;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_idps_daily_kpis_mosip_date
  ON bi.mv_idps_daily_kpis_mosip (activity_date);

CREATE INDEX IF NOT EXISTS ix_mv_idps_daily_kpis_mosip_date
  ON bi.mv_idps_daily_kpis_mosip (activity_date);

COMMENT ON MATERIALIZED VIEW bi.mv_idps_daily_kpis_mosip IS
'Cube KPI IDPS journalier (global) : ordres reçus MosIP (1er jour workflow), impressions en attente fin de journée, volumes erreurs QC/SUP/PERSO.';

-- ---------------------------------------------------------------------------
-- 2) Cube par centre : drill-down API (carte, filtres géo)
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED VIEW bi.mv_idps_daily_kpis_mosip_by_center AS
WITH first_evt AS (
  SELECT DISTINCT ON (w.request_id)
    w.request_id,
    date(w.event_timestamp) AS activity_date,
    w.destination_code AS center_code
  FROM idps.workflow_events w
  ORDER BY w.request_id, w.event_timestamp ASC, w.id ASC
),
received AS (
  SELECT
    f.activity_date,
    f.center_code,
    count(*)::bigint AS orders_received_from_mosip
  FROM first_evt f
  GROUP BY f.activity_date, f.center_code
),
pending AS (
  SELECT
    m.kpi_date AS activity_date,
    m.center_code,
    sum(m.request_count)::bigint AS pending_impressions
  FROM bi.mv_idps_print_orders_daily_center_status m
  WHERE m.status_final = 'PENDING'
  GROUP BY m.kpi_date, m.center_code
),
err AS (
  SELECT
    date(e.event_timestamp) AS activity_date,
    e.destination_code AS center_code,
    count(*) FILTER (WHERE e.error_category = 'QC_ERROR')::bigint AS errors_qc,
    count(*) FILTER (WHERE e.error_category = 'SUP_ERROR')::bigint AS errors_sup,
    count(*) FILTER (WHERE e.error_category = 'PERSO_ERROR')::bigint AS errors_perso
  FROM idps.error_events e
  GROUP BY date(e.event_timestamp), e.destination_code
),
days AS (
  SELECT activity_date, center_code FROM received
  UNION
  SELECT activity_date, center_code FROM pending
  UNION
  SELECT activity_date, center_code FROM err
)
SELECT
  d.activity_date,
  d.center_code,
  coalesce(r.orders_received_from_mosip, 0)::bigint AS orders_received_from_mosip,
  coalesce(p.pending_impressions, 0)::bigint AS pending_impressions,
  coalesce(e.errors_qc, 0)::bigint AS errors_qc,
  coalesce(e.errors_sup, 0)::bigint AS errors_sup,
  coalesce(e.errors_perso, 0)::bigint AS errors_perso
FROM days d
LEFT JOIN received r
  ON r.activity_date = d.activity_date AND r.center_code IS NOT DISTINCT FROM d.center_code
LEFT JOIN pending p
  ON p.activity_date = d.activity_date AND p.center_code IS NOT DISTINCT FROM d.center_code
LEFT JOIN err e
  ON e.activity_date = d.activity_date AND e.center_code IS NOT DISTINCT FROM d.center_code;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_idps_daily_kpis_mosip_by_center
  ON bi.mv_idps_daily_kpis_mosip_by_center (activity_date, center_code);

CREATE INDEX IF NOT EXISTS ix_mv_idps_daily_kpis_mosip_by_center_date
  ON bi.mv_idps_daily_kpis_mosip_by_center (activity_date);

CREATE INDEX IF NOT EXISTS ix_mv_idps_daily_kpis_mosip_by_center_code
  ON bi.mv_idps_daily_kpis_mosip_by_center (center_code);

COMMENT ON MATERIALIZED VIEW bi.mv_idps_daily_kpis_mosip_by_center IS
'Même logique que mv_idps_daily_kpis_mosip, grain activity_date + center_code (destination_code).';

-- ---------------------------------------------------------------------------
-- 3) Vue API : cube par centre + libellés géo (join bi.locations)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_idps_daily_kpis_mosip_geo AS
SELECT
  m.activity_date,
  m.center_code,
  m.orders_received_from_mosip,
  m.pending_impressions,
  m.errors_qc,
  m.errors_sup,
  m.errors_perso,
  c.center_name,
  c.region_code,
  c.prefecture_code,
  c.commune_code,
  c.canton_code,
  c.locality_code,
  c.region_name,
  c.prefecture_name,
  c.commune_name,
  c.canton_name,
  c.locality_name
FROM bi.mv_idps_daily_kpis_mosip_by_center m
LEFT JOIN bi.locations c
  ON lpad(c.center_code, 9, '0') = m.center_code;
