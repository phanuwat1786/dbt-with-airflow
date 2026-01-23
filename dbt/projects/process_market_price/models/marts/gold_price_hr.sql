WITH gold as (
    SELECT * FROM {{ ref('stg_convert_gold_timestamp') }}
)

SELECT MAX(price) as price, currency, unit, timestamp::date + MAKE_TIME(DATE_PART('hour',timestamp)::int,0,00.0) as rounded_timestamp FROM gold
GROUP BY currency, unit, rounded_timestamp
ORDER BY rounded_timestamp
