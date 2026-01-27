{{ config(severity = 'warn' ) }}

SELECT * FROM {{ ref("gold_price_1day_change") }} where DATE_PART('hour',close_price_timestamp) != 3