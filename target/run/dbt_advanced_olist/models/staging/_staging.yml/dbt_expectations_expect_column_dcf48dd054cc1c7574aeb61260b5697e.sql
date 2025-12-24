
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `big-query-dbt-481111`.`dbt_dev_yourname_dbt_test__audit`.`dbt_expectations_expect_column_dcf48dd054cc1c7574aeb61260b5697e`
    
      
    ) dbt_internal_test