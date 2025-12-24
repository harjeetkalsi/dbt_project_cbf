
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`relationships_stg_order_items_85fcc371bcae46211ede4a7fca07283f`
    
      
    ) dbt_internal_test