
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`dbt_expectations_expect_column_aaa015c6d3359922ba0f4662e04bf3c5`
    
      
    ) dbt_internal_test