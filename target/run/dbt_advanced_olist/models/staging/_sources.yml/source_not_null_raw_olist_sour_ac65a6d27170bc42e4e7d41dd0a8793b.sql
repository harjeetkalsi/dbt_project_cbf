
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `arcane-pillar-485809-b6`.`raw_olist_dbt_test__audit`.`source_not_null_raw_olist_sour_ac65a6d27170bc42e4e7d41dd0a8793b`
    
      
    ) dbt_internal_test