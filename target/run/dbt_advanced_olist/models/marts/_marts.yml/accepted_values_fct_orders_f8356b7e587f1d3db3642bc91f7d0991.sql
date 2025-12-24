
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`accepted_values_fct_orders_f8356b7e587f1d3db3642bc91f7d0991`
    
      
    ) dbt_internal_test