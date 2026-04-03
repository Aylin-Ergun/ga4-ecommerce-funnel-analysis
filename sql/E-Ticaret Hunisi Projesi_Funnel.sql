-- E-Ticaret Hunisi Projesi - Funnel Tablosu

WITH
  base_table AS (
    SELECT
      parse_date('%Y%m%d', event_date) AS event_date,
      timestamp_micros(event_timestamp) AS event_timestamp,
      event_name,
      user_pseudo_id,
      concat(
        user_pseudo_id,
        '-',
        CAST(
          (
            SELECT value.int_value
            FROM UNNEST(event_params)
            WHERE key = 'ga_session_id'
          )
          AS string)) AS session_key,
      regexp_extract(
        (
          SELECT value.string_value
          FROM UNNEST(event_params)
          WHERE key = 'page_location'
        ),
        r'^https?://[^/]+(/[^?]*)') AS page_path,
      device.category AS device_category,
      device.language AS device_language,
      device.operating_system AS operating_system,
      traffic_source.source AS traffic_source,
      traffic_source.medium AS traffic_medium,
      traffic_source.name AS traffic_campaign
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _table_suffix BETWEEN '20210101' AND '20211231'
  ),
  session_base AS (
    SELECT
      session_key,
      min(event_timestamp) AS session_start_time,
      min(event_date) AS session_date,
      ARRAY_AGG(page_path IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS landing_page,
      ARRAY_AGG(device_category IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS device_category,
      ARRAY_AGG(device_language IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS device_language,
      ARRAY_AGG(operating_system IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS operating_system,
      ARRAY_AGG(traffic_source IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS traffic_source,
      ARRAY_AGG(traffic_medium IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS traffic_medium,
      ARRAY_AGG(traffic_campaign IGNORE NULLS ORDER BY event_timestamp LIMIT 1)[
        SAFE_OFFSET(0)] AS traffic_campaign,
      max(CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END)
        AS has_session_start,
      max(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END)
        AS has_view_item,
      max(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END)
        AS has_add_to_cart,
      max(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END)
        AS has_begin_checkout,
      max(CASE WHEN event_name = 'add_shipping_info' THEN 1 ELSE 0 END)
        AS has_add_shipping_info,
      max(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END)
        AS has_add_payment_info,
      max(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS has_purchase,
    FROM base_table
    WHERE session_key IS NOT NULL
    GROUP BY session_key
  ),
  funnel_steps AS (
    SELECT 1 AS step_order, 'session_start' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_session_start = 1
    UNION ALL
    SELECT 2 AS step_order, 'view_item' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_view_item = 1
    UNION ALL
    SELECT 3 AS step_order, 'add_to_cart' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_add_to_cart = 1
    UNION ALL
    SELECT 4 AS step_order, 'begin_checkout' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_begin_checkout = 1
    UNION ALL
    SELECT
      5 AS step_order, 'add_shipping_info' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_add_shipping_info = 1
    UNION ALL
    SELECT
      6 AS step_order, 'add_payment_info' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_add_payment_info = 1
    UNION ALL
    SELECT 7 AS step_order, 'purchase' AS step_name, COUNT(*) AS sessions
    FROM session_base
    WHERE has_purchase = 1
  )
SELECT
  step_order,
  step_name,
  sessions,
  round(
    safe_divide(
      sessions, max(CASE WHEN step_order = 1 THEN sessions END) OVER ())
      * 100,
    2) AS conversion_from_start_pct,
  round(
    safe_divide(sessions, lag(sessions) OVER (ORDER BY step_order)) * 100, 2)
    AS conversion_from_prev_step_pct
FROM funnel_steps
ORDER BY step_order;
