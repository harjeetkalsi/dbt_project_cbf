{{
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

/*
  STAGING MODEL: stg_customers
  
  Purpose: Clean and standardize customer data
  Source: Olist Brazilian E-commerce
  
  Transformations:
  - Standardize location data
  - Clean state codes
*/

with source as (
    select * from {{ source('raw_olist_source', 'customers') }}
),

renamed as (
    select
        -- Primary Key
        customer_id,
        
        -- Alternative Key (same customer across orders)
        customer_unique_id,
        
        -- Location
        lower(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        cast(customer_zip_code_prefix as string) as customer_zip_code,
        
        -- Derived: Region classification
        case upper(trim(customer_state))
            when 'SP' then 'Southeast'
            when 'RJ' then 'Southeast'
            when 'MG' then 'Southeast'
            when 'ES' then 'Southeast'
            when 'PR' then 'South'
            when 'SC' then 'South'
            when 'RS' then 'South'
            when 'BA' then 'Northeast'
            when 'PE' then 'Northeast'
            when 'CE' then 'Northeast'
            when 'MA' then 'Northeast'
            when 'PB' then 'Northeast'
            when 'RN' then 'Northeast'
            when 'PI' then 'Northeast'
            when 'AL' then 'Northeast'
            when 'SE' then 'Northeast'
            when 'GO' then 'Central-West'
            when 'DF' then 'Central-West'
            when 'MT' then 'Central-West'
            when 'MS' then 'Central-West'
            else 'North'
        end as customer_region

    from source
)

select * from renamed

{{ limit_in_dev() }}
