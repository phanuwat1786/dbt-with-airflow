WITH current_hr_price AS (

    SELECT price,rounded_timestamp 
    FROM {{ ref("gold_price_hr") }} 
    ORDER BY rounded_timestamp DESC LIMIT 1

    ),
    prev_hr_price AS (
        SELECT price,rounded_timestamp 
        FROM {{ ref("gold_price_hr") }}
        WHERE rounded_timestamp < (SELECT rounded_timestamp FROM current_hr_price)
        ORDER BY rounded_timestamp DESC LIMIT 1
    ),
    prev_and_current_hr_price AS (
        SELECT (
            SELECT price FROM current_hr_price
        ) AS current_hr_price,
        (
            SELECT rounded_timestamp FROM current_hr_price
        ) AS current_hr_price_timestamp,
        (
            SELECT price FROM prev_hr_price
        ) AS prev_hr_price,
        (
            SELECT rounded_timestamp FROM prev_hr_price
        ) AS prev_hr_price_timestamp
    ),
    one_hr_change AS (
        SELECT *,
            ((current_hr_price - prev_hr_price)/prev_hr_price *100)::NUMERIC(5,2) AS "one-hr-change"
        FROM prev_and_current_hr_price
    )

SELECT *,
    CASE 
        WHEN "one-hr-change" > 0 THEN '#188038'
        WHEN "one-hr-change" < 0 THEN '#D93025'
        ELSE '#5F6368'
    END AS font_color,
    CASE 
        WHEN "one-hr-change" > 0 THEN '#E6F4EA'
        WHEN "one-hr-change" < 0 THEN '#FCE8E6'
        ELSE '#F1F3F4'
    END AS bg_color
FROM one_hr_change