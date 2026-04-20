-- Analyse exploratoire des donnees IDPS avant implementation KPI
-- Scope:
--   - idps.workflow_events
--   - idps.error_events
--   - bi.locations (couverture jointure geographique)
--
-- Usage:
--   psql -d <db> -f db-scripts/analyze_idps_tables.sql
--   ou execution bloc par bloc dans DBeaver/DataGrip.

-- ---------------------------------------------------------------------------
-- A) Volumetrie et couverture temporelle (UTC)
-- ---------------------------------------------------------------------------
SELECT
  'workflow_events' AS table_name,
  count(*)::bigint AS row_count,
  min(event_timestamp) AS min_event_ts,
  max(event_timestamp) AS max_event_ts,
  min((event_timestamp AT TIME ZONE 'UTC')::date) AS min_utc_day,
  max((event_timestamp AT TIME ZONE 'UTC')::date) AS max_utc_day
FROM idps.workflow_events

UNION ALL

SELECT
  'error_events' AS table_name,
  count(*)::bigint AS row_count,
  min(event_timestamp) AS min_event_ts,
  max(event_timestamp) AS max_event_ts,
  min((event_timestamp AT TIME ZONE 'UTC')::date) AS min_utc_day,
  max((event_timestamp AT TIME ZONE 'UTC')::date) AS max_utc_day
FROM idps.error_events;

-- ---------------------------------------------------------------------------
-- B) Qualite des cles et colonnes critiques
-- ---------------------------------------------------------------------------
SELECT
  count(*) FILTER (WHERE request_id IS NULL) AS workflow_request_id_null,
  count(*) FILTER (WHERE event_timestamp IS NULL) AS workflow_event_ts_null,
  count(*) FILTER (WHERE destination_code IS NULL OR trim(destination_code) = '') AS workflow_destination_code_empty
FROM idps.workflow_events;

SELECT
  count(*) FILTER (WHERE request_id IS NULL) AS error_request_id_null,
  count(*) FILTER (WHERE event_timestamp IS NULL) AS error_event_ts_null,
  count(*) FILTER (WHERE destination_code IS NULL OR trim(destination_code) = '') AS error_destination_code_empty,
  count(*) FILTER (WHERE error_category IS NULL OR trim(error_category) = '') AS error_category_empty
FROM idps.error_events;

-- ---------------------------------------------------------------------------
-- C) Valeurs enumerees observees vs spec
-- ---------------------------------------------------------------------------
SELECT status, count(*)::bigint AS row_count
FROM idps.workflow_events
GROUP BY status
ORDER BY row_count DESC, status;

SELECT error_category, count(*)::bigint AS row_count
FROM idps.error_events
GROUP BY error_category
ORDER BY row_count DESC, error_category;

-- ---------------------------------------------------------------------------
-- D) Activite quotidienne (UTC) - derniers jours
-- ---------------------------------------------------------------------------
WITH daily_workflow AS (
  SELECT
    (event_timestamp AT TIME ZONE 'UTC')::date AS day_utc,
    count(*)::bigint AS workflow_rows,
    count(DISTINCT request_id)::bigint AS workflow_distinct_requests
  FROM idps.workflow_events
  GROUP BY 1
),
daily_errors AS (
  SELECT
    (event_timestamp AT TIME ZONE 'UTC')::date AS day_utc,
    count(*)::bigint AS error_rows,
    count(DISTINCT request_id)::bigint AS error_distinct_requests
  FROM idps.error_events
  GROUP BY 1
),
days AS (
  SELECT day_utc FROM daily_workflow
  UNION
  SELECT day_utc FROM daily_errors
)
SELECT
  d.day_utc,
  coalesce(w.workflow_rows, 0) AS workflow_rows,
  coalesce(w.workflow_distinct_requests, 0) AS workflow_distinct_requests,
  coalesce(e.error_rows, 0) AS error_rows,
  coalesce(e.error_distinct_requests, 0) AS error_distinct_requests
FROM days d
LEFT JOIN daily_workflow w ON w.day_utc = d.day_utc
LEFT JOIN daily_errors e ON e.day_utc = d.day_utc
ORDER BY d.day_utc DESC
LIMIT 30;

-- ---------------------------------------------------------------------------
-- E) Reinjectons potentielles (request_id avec premiere apparition puis activite ulterieure)
-- ---------------------------------------------------------------------------
WITH request_bounds AS (
  SELECT
    request_id,
    min((event_timestamp AT TIME ZONE 'UTC')::date) AS first_day_utc,
    max((event_timestamp AT TIME ZONE 'UTC')::date) AS last_day_utc,
    count(*)::bigint AS workflow_rows
  FROM idps.workflow_events
  GROUP BY request_id
)
SELECT
  count(*)::bigint AS total_requests,
  count(*) FILTER (WHERE first_day_utc <> last_day_utc)::bigint AS requests_multi_day,
  round(
    100.0 * count(*) FILTER (WHERE first_day_utc <> last_day_utc) / nullif(count(*), 0),
    2
  ) AS pct_requests_multi_day
FROM request_bounds;

-- Top 20 request_id qui s'etendent sur le plus de jours
WITH request_bounds AS (
  SELECT
    request_id,
    min((event_timestamp AT TIME ZONE 'UTC')::date) AS first_day_utc,
    max((event_timestamp AT TIME ZONE 'UTC')::date) AS last_day_utc,
    count(*)::bigint AS workflow_rows
  FROM idps.workflow_events
  GROUP BY request_id
)
SELECT
  request_id,
  first_day_utc,
  last_day_utc,
  (last_day_utc - first_day_utc) AS spread_days,
  workflow_rows
FROM request_bounds
WHERE first_day_utc <> last_day_utc
ORDER BY spread_days DESC, workflow_rows DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- F) Cohabitation statuts sur une meme journee/request_id (bruit potentiel)
-- ---------------------------------------------------------------------------
WITH wf AS (
  SELECT
    request_id,
    (event_timestamp AT TIME ZONE 'UTC')::date AS day_utc,
    count(DISTINCT status) AS distinct_statuses
  FROM idps.workflow_events
  GROUP BY request_id, (event_timestamp AT TIME ZONE 'UTC')::date
)
SELECT
  count(*)::bigint AS request_day_pairs,
  count(*) FILTER (WHERE distinct_statuses > 1)::bigint AS request_day_with_multiple_statuses
FROM wf;

-- ---------------------------------------------------------------------------
-- G) Couverture geographique: destination_code vs bi.locations
-- ---------------------------------------------------------------------------
WITH wf_centers AS (
  SELECT DISTINCT lpad(destination_code, 9, '0') AS center_code_9
  FROM idps.workflow_events
  WHERE destination_code IS NOT NULL AND trim(destination_code) <> ''
),
err_centers AS (
  SELECT DISTINCT lpad(destination_code, 9, '0') AS center_code_9
  FROM idps.error_events
  WHERE destination_code IS NOT NULL AND trim(destination_code) <> ''
),
all_centers AS (
  SELECT center_code_9 FROM wf_centers
  UNION
  SELECT center_code_9 FROM err_centers
)
SELECT
  count(*)::bigint AS distinct_centers_in_events,
  count(*) FILTER (WHERE loc.center_code IS NOT NULL)::bigint AS matched_in_locations,
  count(*) FILTER (WHERE loc.center_code IS NULL)::bigint AS missing_in_locations
FROM all_centers c
LEFT JOIN bi.locations loc
  ON lpad(loc.center_code, 9, '0') = c.center_code_9;

-- Exemples de centers non resolus
WITH wf_centers AS (
  SELECT DISTINCT lpad(destination_code, 9, '0') AS center_code_9
  FROM idps.workflow_events
  WHERE destination_code IS NOT NULL AND trim(destination_code) <> ''
),
err_centers AS (
  SELECT DISTINCT lpad(destination_code, 9, '0') AS center_code_9
  FROM idps.error_events
  WHERE destination_code IS NOT NULL AND trim(destination_code) <> ''
),
all_centers AS (
  SELECT center_code_9 FROM wf_centers
  UNION
  SELECT center_code_9 FROM err_centers
)
SELECT c.center_code_9
FROM all_centers c
LEFT JOIN bi.locations loc
  ON lpad(loc.center_code, 9, '0') = c.center_code_9
WHERE loc.center_code IS NULL
ORDER BY c.center_code_9
LIMIT 50;
