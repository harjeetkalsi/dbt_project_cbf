
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_unique_raw_olist_produc_11c3b793983c138c84181f28f2107efd`
    
      
    ) dbt_internal_test