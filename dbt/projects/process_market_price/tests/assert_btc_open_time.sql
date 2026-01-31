{{ config(severity = 'warn' ) }}
SELECT * FROM {{ ref("btc_price_day_and_hr_change") }} WHERE DATE_PART('hour',open_time AT TIME ZONE 'Asia/Bangkok' AT TIME ZONE 'UCT') != 0