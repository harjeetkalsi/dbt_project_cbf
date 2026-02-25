{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

/*
  STAGING MODEL: stg_order_items
  
  Purpose: Clean order line item data
  Source: Olist Brazilian E-commerce
  
  Transformations:
  - Rename columns
  - Calculate item total (price + freight)
  - Generate surrogate key for unique identification
*/

with source as (
    select * from {{ source('raw_olist_source', 'order_items') }}
),

renamed as (
    select
        -- Surrogate Key (composite primary key)
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_key,
        
        -- Foreign Keys
        order_id,
        product_id,
        seller_id,
        
        -- Item Position
        order_item_id as item_sequence,
        
        -- Pricing
        cast(price as numeric) as item_price,
        cast(freight_value as numeric) as item_freight,
        
        -- Derived: Total for this item
        cast(price as numeric) + cast(freight_value as numeric) as item_total,
        
        -- Shipping date
        cast(shipping_limit_date as timestamp) as shipping_limit_at

    from source
)

select * from renamed

{{ limit_in_dev() }}
