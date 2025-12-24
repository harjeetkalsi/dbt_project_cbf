

select
    subtotal as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`fct_orders`
where subtotal < 0
group by 1

