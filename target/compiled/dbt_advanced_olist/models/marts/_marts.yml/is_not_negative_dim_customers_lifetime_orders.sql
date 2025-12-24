

select
    lifetime_orders as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
where lifetime_orders < 0
group by 1

