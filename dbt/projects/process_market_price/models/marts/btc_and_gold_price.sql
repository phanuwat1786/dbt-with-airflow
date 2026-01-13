WITH 
    gold AS (
        SELECT * FROM {{ ref("stg_convert_gold_timestamp") }}
    ),
    btc AS (
        SELECT * FROM {{ ref("stg_select_btc") }}
    )

SELECT * FROM gold 
UNION ALL 
SELECT * FROM btc