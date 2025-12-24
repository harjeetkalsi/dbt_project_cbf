
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`source_not_null_raw_olist_prod_636edc0d847c30085d124dcdac732f5d`
    
      
    ) dbt_internal_test