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
      MIN(event_timestamp) AS session_start_time,
      MIN(event_date) AS session_date,
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
      MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END)
        AS has_view_item,
      MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END)
        AS has_add_to_cart,
      MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END)
        AS has_begin_checkout,
      MAX(CASE WHEN event_name = 'add_shipping_info' THEN 1 ELSE 0 END)
        AS has_add_shipping_info,
      MAX(CASE WHEN event_name = 'add_payment_info' THEN 1 ELSE 0 END)
        AS has_add_payment_info,
      MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM base_table
    WHERE session_key IS NOT NULL
    GROUP BY session_key
  )
SELECT *
FROM session_base;
