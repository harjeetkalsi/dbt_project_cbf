
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_accepted_values_raw_oli_3aaab13a32ba6e449bc97a63ea11fa37`
    
      
    ) dbt_internal_test