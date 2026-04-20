-- Stats IDPS pour exposition API (couche agrégée au-dessus de 005/006).
--
-- Prérequis : schéma bi, bi.locations, idps.workflow_events / idps.error_events,
--             et exécution préalable de 005 (ou 006) pour :
--             - bi.v_idps_request_events_normalized
--             - bi.v_idps_request_last_status_current
--             - bi.v_idps_request_status_intervals
--             - bi.mv_idps_print_orders_daily_center_status (+ index unique)
--
-- Ordre de refresh recommandé :
--   1) REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_orders_daily_center_status;
--   2) REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_kpis_daily;
--   3) REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_current_by_center_status;

CREATE SCHEMA IF NOT EXISTS bi;

-- ---------------------------------------------------------------------------
-- 1) KPI journaliers globaux (1 ligne / jour) — lecture API rapide
--    Aligné conceptuellement avec IdpsKpiSnapshot côté dashboard :
--    - qc_ko_cards = QC_ERROR + SUP_ERROR (équivalent statut UI « QC_KO »)
--    - error_rate_pct = (qc_ko_cards + print_error_cards) / total × 100
--      (même logique que idps-data.service.ts : erreurs hors « pending »)
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS bi.mv_idps_print_kpis_daily;

CREATE MATERIALIZED VIEW bi.mv_idps_print_kpis_daily AS
SELECT
  d.kpi_date,
  d.total_orders_from_mosip,
  d.pending_orders,
  d.shipped_cards,
  d.qc_error_cards,
  d.sup_error_cards,
  (d.qc_error_cards + d.sup_error_cards)::bigint AS qc_ko_cards,
  d.print_error_cards,
  d.processing_rate_pct,
  CASE
    WHEN d.total_orders_from_mosip = 0 THEN 0::numeric
    ELSE round(
      100.0 * (d.qc_error_cards + d.sup_error_cards + d.print_error_cards)
      / d.total_orders_from_mosip,
      2
    )
  END AS error_rate_pct
FROM (
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
  GROUP BY kpi_date
) d;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_idps_print_kpis_daily_date
  ON bi.mv_idps_print_kpis_daily (kpi_date);

CREATE INDEX IF NOT EXISTS ix_mv_idps_print_kpis_daily_date
  ON bi.mv_idps_print_kpis_daily (kpi_date);

-- ---------------------------------------------------------------------------
-- 2) Snapshot « état courant » par centre (dernier événement connu / requête)
--    Grain : center_code + status_final — filtres géo via join API sur bi.locations
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS bi.v_idps_print_kpis_current;

DROP MATERIALIZED VIEW IF EXISTS bi.mv_idps_print_current_by_center_status;

CREATE MATERIALIZED VIEW bi.mv_idps_print_current_by_center_status AS
SELECT
  c.destination_code AS center_code,
  c.status_final,
  count(*)::bigint AS request_count
FROM bi.v_idps_request_last_status_current c
GROUP BY c.destination_code, c.status_final;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_idps_print_current_center_status
  ON bi.mv_idps_print_current_by_center_status (center_code, status_final);

CREATE INDEX IF NOT EXISTS ix_mv_idps_print_current_center
  ON bi.mv_idps_print_current_by_center_status (center_code);

CREATE INDEX IF NOT EXISTS ix_mv_idps_print_current_status
  ON bi.mv_idps_print_current_by_center_status (status_final);

-- ---------------------------------------------------------------------------
-- 3) Vue lecture seule : KPI courants globaux (agrégat sur snapshot centre)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW bi.v_idps_print_kpis_current AS
SELECT
  sum(request_count)::bigint AS total_orders_from_mosip,
  sum(request_count) FILTER (WHERE status_final = 'PENDING')::bigint AS pending_orders,
  sum(request_count) FILTER (WHERE status_final = 'SHIPPED')::bigint AS shipped_cards,
  sum(request_count) FILTER (WHERE status_final = 'QC_ERROR')::bigint AS qc_error_cards,
  sum(request_count) FILTER (WHERE status_final = 'SUP_ERROR')::bigint AS sup_error_cards,
  (sum(request_count) FILTER (WHERE status_final IN ('QC_ERROR', 'SUP_ERROR')))::bigint AS qc_ko_cards,
  sum(request_count) FILTER (WHERE status_final = 'PRINT_ERROR')::bigint AS print_error_cards,
  CASE
    WHEN sum(request_count) = 0 THEN 0::numeric
    ELSE round(
      100.0 * sum(request_count) FILTER (WHERE status_final = 'SHIPPED')
      / sum(request_count),
      2
    )
  END AS processing_rate_pct,
  CASE
    WHEN sum(request_count) = 0 THEN 0::numeric
    ELSE round(
      100.0 * (
        sum(request_count) FILTER (WHERE status_final IN ('QC_ERROR', 'SUP_ERROR'))
        + sum(request_count) FILTER (WHERE status_final = 'PRINT_ERROR')
      )
      / sum(request_count),
      2
    )
  END AS error_rate_pct
FROM bi.mv_idps_print_current_by_center_status;

-- Après chargement des données sources :
-- REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_orders_daily_center_status;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_kpis_daily;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY bi.mv_idps_print_current_by_center_status;
-- (lecture KPI courant : SELECT * FROM bi.v_idps_print_kpis_current;)
-- (série temporelle : SELECT * FROM bi.mv_idps_print_kpis_daily WHERE kpi_date BETWEEN :from AND :to;)
