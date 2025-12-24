
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_relationships_raw_olist_0ce611a7afc62c8655f9c0e4c89af0bd`
    
      
    ) dbt_internal_test