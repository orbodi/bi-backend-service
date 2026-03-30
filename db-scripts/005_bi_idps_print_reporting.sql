-- Reporting IDPS impressions (historique journalier "as-of").
-- Règles métier validées :
-- 1) Statuts erreurs distincts (PERSO/QC/SUP séparés)
-- 2) Mapping géographique via destination_code = bi.locations.center_code
-- 3) Jour calculé selon fuseau de la session DB (timestamptz::date)

CREATE SCHEMA IF NOT EXISTS bi;

-- ---------------------------------------------------------------------------
-- 1) Flux normalisé des événements IDPS
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_idps_request_events_normalized AS
SELECT
  w.id::bigint AS event_id,
  w.request_id,
  w.event_timestamp,
  w.destination_code,
  w.document_type,
  w.file_name,
  w.ingested_at,
  'WORKFLOW'::text AS event_source,
  w.status::text AS workflow_status,
  NULL::text AS error_category,
  CASE
    WHEN w.status = 'FINISH' THEN 'SHIPPED'
    WHEN w.status = 'BACKLOG' THEN 'PENDING'
    ELSE 'WORKFLOW_OTHER'
  END::text AS status_final
FROM idps.workflow_events w

UNION ALL

SELECT
  e.id::bigint AS event_id,
  e.request_id,
  e.event_timestamp,
  e.destination_code,
  e.document_type,
  e.file_name,
  e.ingested_at,
  'ERROR'::text AS event_source,
  NULL::text AS workflow_status,
  e.error_category::text AS error_category,
  CASE
    WHEN e.error_category = 'PERSO_ERROR' THEN 'PRINT_ERROR'
    WHEN e.error_category = 'QC_ERROR' THEN 'QC_ERROR'
    WHEN e.error_category = 'SUP_ERROR' THEN 'SUP_ERROR'
    ELSE 'ERROR_OTHER'
  END::text AS status_final
FROM idps.error_events e;

-- ---------------------------------------------------------------------------
-- 2) Dernier statut "courant" par request_id (snapshot instantané)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_idps_request_last_status_current AS
WITH ranked AS (
  SELECT
    n.*,
    row_number() OVER (
      PARTITION BY n.request_id
      ORDER BY n.event_timestamp DESC, n.event_id DESC
    ) AS rn
  FROM bi.v_idps_request_events_normalized n
)
SELECT
  request_id,
  event_timestamp,
  destination_code,
  document_type,
  file_name,
  ingested_at,
  event_source,
  workflow_status,
  error_category,
  status_final
FROM ranked
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- 3) Intervalles de validité du statut par request_id
--    Un événement vaut de event_timestamp jusqu'à l'événement suivant.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_idps_request_status_intervals AS
SELECT
  n.request_id,
  n.event_timestamp AS valid_from_ts,
  lead(n.event_timestamp) OVER (
    PARTITION BY n.request_id
    ORDER BY n.event_timestamp, n.event_id
  ) AS valid_to_ts_exclusive,
  n.destination_code,
  n.status_final,
  n.event_source,
  n.workflow_status,
  n.error_category
FROM bi.v_idps_request_events_normalized n;

-- ---------------------------------------------------------------------------
-- 4) Cube journalier "as-of" au grain centre + statut.
--    Pour chaque jour D, un request_id compte dans le statut valide à D fin de journée.
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS bi.mv_idps_print_orders_daily_center_status;

CREATE MATERIALIZED VIEW bi.mv_idps_print_orders_daily_center_status AS
WITH expanded AS (
  SELECT
    gs.day::date AS kpi_date,
    i.request_id,
    i.destination_code AS center_code,
    i.status_final
  FROM bi.v_idps_request_status_intervals i
  CROSS JOIN LATERAL generate_series(
    i.valid_from_ts::date,
    (
      COALESCE(i.valid_to_ts_exclusive, now())
      - interval '1 microsecond'
    )::date,
    interval '1 day'
  ) AS gs(day)
)
SELECT
  e.kpi_date,
  e.center_code,
  e.status_final,
  count(*)::bigint AS request_count
FROM expanded e
GROUP BY e.kpi_date, e.center_code, e.status_final;

CREATE INDEX IF NOT EXISTS ix_mv_print_daily_center_status_date
  ON bi.mv_idps_print_orders_daily_center_status (kpi_date);

CREATE INDEX IF NOT EXISTS ix_mv_print_daily_center_status_center
  ON bi.mv_idps_print_orders_daily_center_status (center_code);

CREATE INDEX IF NOT EXISTS ix_mv_print_daily_center_status_status
  ON bi.mv_idps_print_orders_daily_center_status (status_final);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_print_daily_center_status
  ON bi.mv_idps_print_orders_daily_center_status (kpi_date, center_code, status_final);

-- ---------------------------------------------------------------------------
-- 5) Vue enrichie géographie (join bi.locations), prête API BI
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_print_orders_daily_geo AS
SELECT
  m.kpi_date,
  m.center_code,
  l.center_name,
  l.region_code,
  l.prefecture_code,
  l.commune_code,
  l.canton_code,
  l.locality_code,
  l.region_name,
  l.prefecture_name,
  l.commune_name,
  l.canton_name,
  l.locality_name,
  m.status_final,
  m.request_count
FROM bi.mv_idps_print_orders_daily_center_status m
LEFT JOIN bi.locations l
  ON l.center_code = m.center_code;

-- ---------------------------------------------------------------------------
-- 6) Vue KPI globale par jour (sans filtre géo), prête page impressions globale
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_print_kpis_daily AS
SELECT
  kpi_date,
  sum(request_count)::bigint AS total_orders_from_mosip,
  sum(request_count) FILTER (WHERE status_final = 'PENDING')::bigint AS pending_orders,
  sum(request_count) FILTER (WHERE status_final = 'SHIPPED')::bigint AS shipped_cards,
  sum(request_count) FILTER (WHERE status_final = 'QC_ERROR')::bigint AS qc_error_cards,
  sum(request_count) FILTER (WHERE status_final = 'SUP_ERROR')::bigint AS sup_error_cards,
  sum(request_count) FILTER (WHERE status_final = 'PRINT_ERROR')::bigint AS print_error_cards,
  CASE
    WHEN sum(request_count) = 0 THEN 0::numeric
    ELSE round(
      100.0 * sum(request_count) FILTER (WHERE status_final = 'SHIPPED')
      / sum(request_count),
      2
    )
  END AS processing_rate_pct
FROM bi.mv_idps_print_orders_daily_center_status
GROUP BY kpi_date;

-- Exécution recommandée après création / reload des sources :
-- REFRESH MATERIALIZED VIEW bi.mv_idps_print_orders_daily_center_status;

