
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `arcane-pillar-485809-b6`.`raw_olist_dbt_test__audit`.`dbt_expectations_source_expect_5f19db14015851f8d17beac55a954f20`
    
      
    ) dbt_internal_test