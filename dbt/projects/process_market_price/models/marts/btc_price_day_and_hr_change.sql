WITH market_open_time as (
        SELECT ((NOW() AT TIME ZONE 'UTC')::date + MAKE_TIME(0,0,0.0))::timestamptz AT TIME ZONE 'Asia/Bangkok' as open_time
    ),
    open_price AS (
        SELECT price AS open_price, rounded_timestamp AS open_price_timestamp
        FROM {{ ref("btc_price_hr") }} 
        WHERE rounded_timestamp <= ( SELECT open_time FROM market_open_time ) 
        ORDER BY rounded_timestamp DESC LIMIT 1
    ),
    current_price AS (
        SELECT price AS current_price, rounded_timestamp AS current_price_timestamp
        FROM {{ ref("btc_price_hr") }} 
        ORDER BY rounded_timestamp DESC LIMIT 1
    ),
    prev_hr_price AS (
        SELECT price AS prev_hr_price, rounded_timestamp AS prev_hr_price_timestamp
        FROM {{ ref("btc_price_hr") }}
        WHERE rounded_timestamp < (SELECT current_price_timestamp FROM current_price)
        ORDER BY rounded_timestamp DESC LIMIT 1 
    ),
    change AS (
        SELECT *,
            ((current_price - open_price)/open_price * 100)::NUMERIC(5,2) AS "one-day-change",
            ((current_price - prev_hr_price)/prev_hr_price * 100)::NUMERIC(5,2) AS "one-hr-change" 
        FROM market_open_time
        CROSS JOIN open_price
        CROSS JOIN current_price
        CROSS JOIN prev_hr_price
    )

SELECT  *,
        CASE 
            WHEN "one-day-change" > 0 THEN '#188038'
            WHEN "one-day-change" < 0 THEN '#D93025'
            ELSE '#5F6368'
        END AS day_font_color,
        CASE 
            WHEN "one-day-change" > 0 THEN '#E6F4EA'
            WHEN "one-day-change" < 0 THEN '#FCE8E6'
            ELSE '#F1F3F4'
        END AS day_bg_color,
        CASE 
            WHEN "one-hr-change" > 0 THEN '#188038'
            WHEN "one-hr-change" < 0 THEN '#D93025'
            ELSE '#5F6368'
        END AS hr_font_color,
        CASE 
            WHEN "one-hr-change" > 0 THEN '#E6F4EA'
            WHEN "one-hr-change" < 0 THEN '#FCE8E6'
            ELSE '#F1F3F4'
        END AS hr_bg_color
    FROM change