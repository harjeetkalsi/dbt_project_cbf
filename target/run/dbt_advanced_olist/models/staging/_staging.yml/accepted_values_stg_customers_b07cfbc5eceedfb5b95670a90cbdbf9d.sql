
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `arcane-pillar-485809-b6`.`raw_olist_dbt_test__audit`.`accepted_values_stg_customers_b07cfbc5eceedfb5b95670a90cbdbf9d`
    
      
    ) dbt_internal_test