
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`dbt_expectations_source_expect_45b75121f164c929042f3ce7fc9a7e06`
    
      
    ) dbt_internal_test