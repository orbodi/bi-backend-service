-- Validation de la spec KPI IDPS contre les données réelles.
-- Règles rappelées :
--   - Journée J = date calendaire UTC dérivée de event_timestamp.
--   - Ordres MosIP reçus : première apparition (min(event_timestamp) global) par request_id, comptées le jour J.
--   - État fin de journée J (workflow) : parmi les événements dont la date UTC = J, dernier par request_id
--     (event_timestamp DESC, id DESC).
--   - Erreurs : COUNT(*) des lignes error_events le jour J par error_category (lignes, même request_id).
--   - Pas de rejeu fichier / pas de dédoublonnage spécifique dans cette validation.
--
-- Usage : modifier la date dans params, puis exécuter tout le script (psql, DBeaver, etc.).

WITH params AS (
  SELECT DATE '2026-04-01' AS j  -- <<< remplacer par un jour présent dans vos données
),

-- Jours couverts (aperçu : choisir j dans cette liste si besoin)
days_sample AS (
  SELECT DISTINCT (w.event_timestamp AT TIME ZONE 'UTC')::date AS d
  FROM idps.workflow_events w
  ORDER BY 1 DESC
  LIMIT 14
),

-- 1) Ordres reçus MosIP le jour J (première apparition globale)
first_appearance AS (
  SELECT
    w.request_id,
    (min(w.event_timestamp) AT TIME ZONE 'UTC')::date AS first_day_utc
  FROM idps.workflow_events w
  GROUP BY w.request_id
),
q_orders_received AS (
  SELECT count(*)::bigint AS orders_received_from_mosip
  FROM first_appearance f
  CROSS JOIN params p
  WHERE f.first_day_utc = p.j
),

-- 2) Dernier statut workflow le jour J (uniquement les request_id ayant au moins un événement ce jour-là)
day_workflow AS (
  SELECT w.*
  FROM idps.workflow_events w
  CROSS JOIN params p
  WHERE (w.event_timestamp AT TIME ZONE 'UTC')::date = p.j
),
last_status_day AS (
  SELECT DISTINCT ON (dw.request_id)
    dw.request_id,
    dw.status AS end_of_day_status
  FROM day_workflow dw
  CROSS JOIN params p
  ORDER BY dw.request_id, dw.event_timestamp DESC, dw.id DESC
),
q_workflow_end AS (
  SELECT
    l.end_of_day_status,
    count(*)::bigint AS request_count
  FROM last_status_day l
  GROUP BY l.end_of_day_status
  ORDER BY 1
),

-- 3) Erreurs le jour J (lignes)
q_errors AS (
  SELECT
    e.error_category,
    count(*)::bigint AS error_row_count
  FROM idps.error_events e
  CROSS JOIN params p
  WHERE (e.event_timestamp AT TIME ZONE 'UTC')::date = p.j
  GROUP BY e.error_category
  ORDER BY 1
),

-- 4) Même chose par destination_code (grain centre)
q_errors_by_center AS (
  SELECT
    e.destination_code,
    e.error_category,
    count(*)::bigint AS error_row_count
  FROM idps.error_events e
  CROSS JOIN params p
  WHERE (e.event_timestamp AT TIME ZONE 'UTC')::date = p.j
  GROUP BY e.destination_code, e.error_category
  ORDER BY e.destination_code, e.error_category
),

q_workflow_end_by_center AS (
  SELECT
    l.destination_code,
    l.end_of_day_status,
    count(*)::bigint AS request_count
  FROM (
    SELECT DISTINCT ON (dw.request_id)
      dw.request_id,
      dw.destination_code,
      dw.status AS end_of_day_status
    FROM day_workflow dw
    CROSS JOIN params p
    ORDER BY dw.request_id, dw.event_timestamp DESC, dw.id DESC
  ) l
  GROUP BY l.destination_code, l.end_of_day_status
  ORDER BY l.destination_code, l.end_of_day_status
),

q_received_by_center AS (
  SELECT
    x.destination_code_at_first,
    count(*)::bigint AS orders_received_from_mosip
  FROM (
    SELECT
      w.request_id,
      (min(w.event_timestamp) AT TIME ZONE 'UTC')::date AS first_day_utc,
      (array_agg(w.destination_code ORDER BY w.event_timestamp ASC, w.id ASC))[1] AS destination_code_at_first
    FROM idps.workflow_events w
    GROUP BY w.request_id
  ) x
  CROSS JOIN params p
  WHERE x.first_day_utc = p.j
  GROUP BY x.destination_code_at_first
  ORDER BY 1
)

SELECT 'params.j (UTC date)' AS section, p.j::text AS value
FROM params p

UNION ALL
SELECT '--- SAMPLE days workflow (5 most recent UTC days) ---', null::text
FROM params p

UNION ALL
SELECT t.section, t.value
FROM (
  SELECT ('day ' || s.d::text) AS section, null::text AS value
  FROM days_sample s
  ORDER BY s.d DESC
  LIMIT 5
) t

UNION ALL
SELECT '--- 1) ORDERS RECEIVED (first appearance on J) ---', null::text
FROM params p

UNION ALL
SELECT 'orders_received_from_mosip', q.orders_received_from_mosip::text
FROM q_orders_received q

UNION ALL
SELECT '--- 2) WORKFLOW END-OF-DAY J (last event that UTC day) ---', null::text
FROM params p

UNION ALL
SELECT w.end_of_day_status, w.request_count::text
FROM q_workflow_end w

UNION ALL
SELECT '--- 3) ERRORS ROWS ON J ---', null::text
FROM params p

UNION ALL
SELECT e.error_category, e.error_row_count::text
FROM q_errors e

UNION ALL
SELECT '--- (see detailed sections below: run as separate queries or uncomment) ---', null::text
FROM params p;

-- ---------------------------------------------------------------------------
-- Requêtes détaillées : réutiliser le MÊME bloc WITH (params → q_received_by_center)
-- puis une seule ligne SELECT ci-dessous (même session / même statement).
-- ---------------------------------------------------------------------------

-- A) Aperçu des 14 derniers jours UTC (workflow) : reprendre le WITH jusqu'à days_sample uniquement, ou :
-- SELECT * FROM days_sample;  -- (uniquement si le WITH days_sample est dans le même query)

-- B) Erreurs par centre : ... WITH complet ... puis :
-- SELECT * FROM q_errors_by_center;

-- C) Statut fin de journée par centre :
-- SELECT * FROM q_workflow_end_by_center;

-- D) Ordres MosIP reçus le jour J par centre (destination au 1er événement) :
-- SELECT * FROM q_received_by_center;

-- E) Jointure bi.locations (enrichissement) — erreurs jour J par centre
/*
WITH params AS (SELECT DATE '2026-04-01' AS j)
SELECT
  e.destination_code,
  l.center_name,
  l.region_name,
  e.error_category,
  count(*)::bigint AS error_row_count
FROM idps.error_events e
CROSS JOIN params p
LEFT JOIN bi.locations l
  ON lpad(l.center_code, 9, '0') = lpad(e.destination_code, 9, '0')
WHERE (e.event_timestamp AT TIME ZONE 'UTC')::date = p.j
GROUP BY e.destination_code, l.center_name, l.region_name, e.error_category
ORDER BY e.destination_code, e.error_category;
*/

-- F) Jointure bi.locations — dernier statut fin J par centre
/*
WITH params AS (SELECT DATE '2026-04-01' AS j),
day_workflow AS (
  SELECT w.*
  FROM idps.workflow_events w
  CROSS JOIN params p
  WHERE (w.event_timestamp AT TIME ZONE 'UTC')::date = p.j
),
last_per_request AS (
  SELECT DISTINCT ON (dw.request_id)
    dw.request_id,
    dw.destination_code,
    dw.status AS end_of_day_status
  FROM day_workflow dw
  ORDER BY dw.request_id, dw.event_timestamp DESC, dw.id DESC
)
SELECT
  r.destination_code,
  l.center_name,
  l.region_name,
  r.end_of_day_status,
  count(*)::bigint AS request_count
FROM last_per_request r
LEFT JOIN bi.locations l
  ON lpad(l.center_code, 9, '0') = lpad(r.destination_code, 9, '0')
GROUP BY r.destination_code, l.center_name, l.region_name, r.end_of_day_status
ORDER BY r.destination_code, r.end_of_day_status;
*/
