

select
    weight_grams as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_products`
where weight_grams < 0
group by 1

