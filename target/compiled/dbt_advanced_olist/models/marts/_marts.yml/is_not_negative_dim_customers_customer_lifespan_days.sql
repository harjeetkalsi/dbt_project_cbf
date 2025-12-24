

select
    customer_lifespan_days as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
where customer_lifespan_days < 0
group by 1

