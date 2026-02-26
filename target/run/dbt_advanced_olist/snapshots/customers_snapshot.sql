
      
  
    

    create or replace table `arcane-pillar-485809-b6`.`snapshots`.`customers_snapshot`
      
    
    

    
    OPTIONS()
    as (
      
    

    select *,
        to_hex(md5(concat(coalesce(cast(customer_id as string), ''), '|',coalesce(cast(
    current_timestamp()
 as string), '')))) as dbt_scd_id,
        
    current_timestamp()
 as dbt_updated_at,
        
    current_timestamp()
 as dbt_valid_from,
        
  
  coalesce(nullif(
    current_timestamp()
, 
    current_timestamp()
), null)
  as dbt_valid_to
from (
        



/*
  SNAPSHOT: customers_snapshot
  
  Purpose: Track historical changes to customer data (SCD Type 2)
  
  Strategy: 'check' - monitors specified columns for changes
  
  Tracked columns:
  - customer_city
  - customer_state  
  - customer_zip_code
  
  Added columns:
  - dbt_valid_from: When this version became active
  - dbt_valid_to: When this version was superseded (NULL = current)
  - dbt_scd_id: Unique identifier for this version
  
  Usage:
    dbt snapshot
*/

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code,
    customer_region,
    current_timestamp() as _snapshot_at

from `arcane-pillar-485809-b6`.`raw_olist_staging`.`stg_customers`

    ) sbq



    );
  
  