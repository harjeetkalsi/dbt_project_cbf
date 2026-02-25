{{
  config(
    materialized='view',
    tags=['staging', 'products']
  )
}}

/*
  STAGING MODEL: stg_products
  
  Purpose: Clean product catalog data
  Source: Olist Brazilian E-commerce
  
  Transformations:
  - Calculate product volume
  - Classify product size
*/

with source as (
    select * from {{ source('raw_olist_source', 'products') }}
),

renamed as (
    select
        -- Primary Key
        product_id,
        
        -- Category (Portuguese - join with translation table)
        product_category_name as category_name_pt,
        
        -- Dimensions
        cast(product_weight_g as numeric) as weight_grams,
        cast(product_length_cm as numeric) as length_cm,
        cast(product_height_cm as numeric) as height_cm,
        cast(product_width_cm as numeric) as width_cm,
        
        -- Derived: Volume in cubic cm
        cast(product_length_cm as numeric) 
            * cast(product_height_cm as numeric) 
            * cast(product_width_cm as numeric) as volume_cm3,
        
        -- Derived: Size classification
        case
            when cast(product_weight_g as numeric) < 500 then 'Small'
            when cast(product_weight_g as numeric) < 2000 then 'Medium'
            when cast(product_weight_g as numeric) < 10000 then 'Large'
            else 'Extra Large'
        end as size_category,
        
        -- Product description stats
        product_name_lenght as name_length,
        product_description_lenght as description_length,
        product_photos_qty as photo_count

    from source
)

select * from renamed

{{ limit_in_dev() }}
