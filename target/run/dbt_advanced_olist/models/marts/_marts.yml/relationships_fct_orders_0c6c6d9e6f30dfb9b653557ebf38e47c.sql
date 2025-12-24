
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`relationships_fct_orders_0c6c6d9e6f30dfb9b653557ebf38e47c`
    
      
    ) dbt_internal_test