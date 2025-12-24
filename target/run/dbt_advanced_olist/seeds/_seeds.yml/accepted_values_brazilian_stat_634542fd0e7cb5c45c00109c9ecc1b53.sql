
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`accepted_values_brazilian_stat_634542fd0e7cb5c45c00109c9ecc1b53`
    
      
    ) dbt_internal_test