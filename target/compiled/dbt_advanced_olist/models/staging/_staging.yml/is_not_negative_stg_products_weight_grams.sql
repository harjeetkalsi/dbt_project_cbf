

select
    weight_grams as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_products`
where weight_grams < 0
group by 1

