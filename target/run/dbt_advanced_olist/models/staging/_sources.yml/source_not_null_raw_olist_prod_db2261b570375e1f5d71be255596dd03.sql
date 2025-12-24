
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_not_null_raw_olist_prod_db2261b570375e1f5d71be255596dd03`
    
      
    ) dbt_internal_test