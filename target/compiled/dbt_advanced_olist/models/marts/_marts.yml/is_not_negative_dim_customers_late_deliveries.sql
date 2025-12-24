

select
    late_deliveries as failing_value,
    count(*) as row_count
from `big-query-dbt-481111`.`dbt_dev_yourname_marts`.`dim_customers`
where late_deliveries < 0
group by 1

