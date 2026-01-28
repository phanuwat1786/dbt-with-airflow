WITH market_open_time as (
        SELECT ((NOW() AT TIME ZONE 'UTC')::date + MAKE_TIME(0,0,0.0))::timestamptz AT TIME ZONE 'Asia/Bangkok' as open_time
    ),
    btc_price_at_market_open AS (
        SELECT price,rounded_timestamp 
        FROM {{ ref("btc_price_hr") }} 
        WHERE rounded_timestamp = ( SELECT open_time FROM market_open_time )
    ),
    btc_current_price AS (
        SELECT price,rounded_timestamp 
        FROM {{ ref("btc_price_hr") }} 
        ORDER BY rounded_timestamp DESC LIMIT 1
    ),
    detail AS (
        SELECT 
        (
            SELECT open_time FROM market_open_time
        ),
        (
            SELECT price FROM btc_price_at_market_open
        ) AS opentime_price,
        (
            SELECT rounded_timestamp FROM btc_price_at_market_open
        ) AS price_timestamp,
        (
            SELECT price FROM btc_current_price 
        ) AS current_price,
        (
            SELECT rounded_timestamp FROM btc_current_price
        ) AS current_price_timestamp
    )

SELECT *,
    CASE 
        WHEN "one-day-change" > 0 THEN '#188038'
        WHEN "one-day-change" < 0 THEN '#D93025'
        ELSE '#5F6368'
    END AS font_color,
    CASE 
        WHEN "one-day-change" > 0 THEN '#E6F4EA'
        WHEN "one-day-change" < 0 THEN '#FCE8E6'
        ELSE '#F1F3F4'
    END AS bg_color
FROM (SELECT *,(current_price - opentime_price)/opentime_price * 100 as "one-day-change" FROM detail) one_daye_change