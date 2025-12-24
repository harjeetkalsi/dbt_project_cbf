
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`accepted_values_dim_customers_3e1b7f0e14f3db765f997aa38f5db476`
    
      
    ) dbt_internal_test