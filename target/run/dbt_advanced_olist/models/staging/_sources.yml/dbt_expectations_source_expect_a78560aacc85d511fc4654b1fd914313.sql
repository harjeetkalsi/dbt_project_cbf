
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`dbt_expectations_source_expect_a78560aacc85d511fc4654b1fd914313`
    
      
    ) dbt_internal_test