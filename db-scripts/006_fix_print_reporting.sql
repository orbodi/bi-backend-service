-- Correctifs reporting impressions:
-- 1) kpi_date au type DATE (et non timestamp)
-- 2) mapping centre via zéro-padding: lpad(bi.locations.center_code, 9, '0')

-- ---------------------------------------------------------------------------
-- 1) Recréer la MV avec kpi_date::date
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
    (i.valid_from_ts)::date::timestamp,
    (
      COALESCE(i.valid_to_ts_exclusive, now()) - interval '1 microsecond'
    )::date::timestamp,
    interval '1 day'
  ) AS gs(day)
)
SELECT
  e.kpi_date::date AS kpi_date,
  e.center_code,
  e.status_final,
  count(*)::bigint AS request_count
FROM expanded e
GROUP BY e.kpi_date::date, e.center_code, e.status_final;

CREATE INDEX IF NOT EXISTS ix_mv_print_daily_center_status_date
  ON bi.mv_idps_print_orders_daily_center_status (kpi_date);

CREATE INDEX IF NOT EXISTS ix_mv_print_daily_center_status_center
  ON bi.mv_idps_print_orders_daily_center_status (center_code);

CREATE INDEX IF NOT EXISTS ix_mv_print_daily_center_status_status
  ON bi.mv_idps_print_orders_daily_center_status (status_final);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_print_daily_center_status
  ON bi.mv_idps_print_orders_daily_center_status (kpi_date, center_code, status_final);

-- ---------------------------------------------------------------------------
-- 2) Corriger la vue enrichie géo (join destination_code <-> center_code)
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
  ON lpad(l.center_code, 9, '0') = m.center_code;

-- ---------------------------------------------------------------------------
-- 3) Recréer la vue KPI journalière
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

-- A lancer ensuite:
-- REFRESH MATERIALIZED VIEW bi.mv_idps_print_orders_daily_center_status;

