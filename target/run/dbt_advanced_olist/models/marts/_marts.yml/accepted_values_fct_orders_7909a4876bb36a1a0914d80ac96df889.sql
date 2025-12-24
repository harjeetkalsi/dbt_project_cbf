
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`accepted_values_fct_orders_7909a4876bb36a1a0914d80ac96df889`
    
      
    ) dbt_internal_test