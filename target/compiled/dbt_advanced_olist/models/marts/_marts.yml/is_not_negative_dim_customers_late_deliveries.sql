

select
    late_deliveries as failing_value,
    count(*) as row_count
from `arcane-pillar-485809-b6`.`raw_olist_marts`.`dim_customers`
where late_deliveries < 0
group by 1

