
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `arcane-pillar-485809-b6`.`raw_olist_dbt_test__audit`.`dbt_expectations_source_expect_ac5e3eaa1cfefd320687dee0bff9cfed`
    
      
    ) dbt_internal_test