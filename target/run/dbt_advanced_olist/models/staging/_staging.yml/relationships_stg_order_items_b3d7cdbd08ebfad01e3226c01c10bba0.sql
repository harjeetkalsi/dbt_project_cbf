
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`relationships_stg_order_items_b3d7cdbd08ebfad01e3226c01c10bba0`
    
      
    ) dbt_internal_test