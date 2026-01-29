WITH market_close_time as (
    SELECT (((NOW() AT TIME ZONE 'Asia/Bangkok')::date + MAKE_TIME(3,0,0.0)) AT TIME ZONE 'Asia/Bangkok') AT TIME ZONE 'Asia/Bangkok' as close_time
    ),
    gold_price_at_market_close AS (
        SELECT price, rounded_timestamp 
        FROM {{ ref('gold_price_hr') }} 
        WHERE rounded_timestamp <= (SELECT close_time FROM market_close_time LIMIT 1)
        ORDER BY rounded_timestamp DESC LIMIT 1
    ),
    current_gold_price AS (
        SELECT price,rounded_timestamp FROM {{ ref('gold_price_hr') }} ORDER BY rounded_timestamp DESC LIMIT 1
    ),
    detail AS (
        SELECT 
            (
                SELECT close_time FROM market_close_time
            ) ,
            (
                SELECT price FROM gold_price_at_market_close
            ) as closetime_price,
            (
                SELECT rounded_timestamp FROM gold_price_at_market_close
            ) as close_price_timestamp,
            (
                SELECT price FROM current_gold_price 
            ) as current_price ,
            (
                SELECT rounded_timestamp FROM current_gold_price
            ) as current_price_timestamp 
    )

SELECT  *,
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
        FROM ( SELECT *, ((current_price - closetime_price)/closetime_price * 100)::NUMERIC(5,2) AS "one-day-change" FROM detail ) one_daye_change