{{ config(severity = 'warn' ) }}

SELECT * FROM {{ ref("btc_price_day_and_hr_change") }} WHERE current_price_timestamp - prev_hr_price_timestamp <> MAKE_TIME( 1,0,0.00 )