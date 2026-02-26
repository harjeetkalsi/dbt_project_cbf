
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `arcane-pillar-485809-b6`.`raw_olist_dbt_test__audit`.`accepted_values_stg_products_6fcf4aab6b8b938367296091ab8a30a3`
    
      
    ) dbt_internal_test