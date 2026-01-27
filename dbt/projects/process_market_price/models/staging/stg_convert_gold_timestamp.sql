WITH gold_price AS (
    SELECT timestamp,price,currency FROM {{ source("market_price","gold") }}
)

SELECT 'gold' AS type, price,currency,'/oz' AS unit,to_timestamp(timestamp) AT TIME ZONE 'Asia/Bangkok' AS timestamp FROM gold_price