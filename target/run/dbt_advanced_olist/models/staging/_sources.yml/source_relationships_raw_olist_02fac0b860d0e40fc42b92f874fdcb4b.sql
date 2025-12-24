
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_relationships_raw_olist_02fac0b860d0e40fc42b92f874fdcb4b`
    
      
    ) dbt_internal_test