{{ config(severity = 'warn' ) }}
SELECT * FROM {{ ref("gold_price_day_and_hr_change") }} WHERE DATE_PART('hour',close_time) != 3