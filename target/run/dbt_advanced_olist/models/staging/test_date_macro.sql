

  create or replace view `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`test_date_macro`
  OPTIONS()
  as 

with sample_dates as (
    select date('2017-01-15') as test_date
    union all
    select date('2017-06-21') as test_date
)

select
    test_date,
    

    extract(year from test_date) as test_year,
    extract(month from test_date) as test_month,
    extract(quarter from test_date) as test_quarter,
    extract(week from test_date) as test_week,
    extract(dayofweek from test_date) as test_day_of_week,
    case 
        when extract(dayofweek from test_date) in (1, 7) then true 
        else false 
    end as is_weekend


from sample_dates;

