WITH bitcoin AS (
    SELECT "USD", create_at_bi FROM {{ source("market_price","bitcoin") }}
)

SELECT 'btc' AS type, "USD" AS price, 'USD' AS currency, '/1 btc' AS unit, create_at_bi as timestamp FROM bitcoin LIMIT 100