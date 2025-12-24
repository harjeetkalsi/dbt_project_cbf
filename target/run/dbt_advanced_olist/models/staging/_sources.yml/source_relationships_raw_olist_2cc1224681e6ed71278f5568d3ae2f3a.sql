
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_relationships_raw_olist_2cc1224681e6ed71278f5568d3ae2f3a`
    
      
    ) dbt_internal_test