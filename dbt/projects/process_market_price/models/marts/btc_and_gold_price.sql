WITH 
    gold AS (
        SELECT * FROM {{ ref("st_convert_gold_timestamp") }}
    ),
    btc AS (
        SELECT * FROM {{ ref("st_select_btc") }}
    )

SELECT * FROM gold 
UNION ALL 
SELECT * FROM btc