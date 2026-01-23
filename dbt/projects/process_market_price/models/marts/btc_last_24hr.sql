WITH btc AS (
    SELECT * FROM {{ ref('stg_select_btc') }}
)

SELECT max(price) as price, currency, unit, timestamp::DATE + MAKE_TIME(DATE_PART('hour',timestamp)::int,0,0.0) as timestamp FROM btc
GROUP BY currency, unit, timestamp
ORDER BY timestamp DESC LIMIT 24