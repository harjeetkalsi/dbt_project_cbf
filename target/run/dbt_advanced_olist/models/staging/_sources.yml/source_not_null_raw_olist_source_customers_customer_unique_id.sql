
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `arcane-pillar-485809-b6`.`raw_olist_dbt_test__audit`.`source_not_null_raw_olist_source_customers_customer_unique_id`
    
      
    ) dbt_internal_test