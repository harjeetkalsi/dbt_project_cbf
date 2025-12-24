
/*
  STAGING MODEL: stg_product_categories
  Purpose: Product category translation (Portuguese → English)
  Source: Olist Brazilian E-commerce
*/
with source as (
    select * from `big-query-dbt-481111`.`raw_olist`.`product_category_name_translation`
),
renamed as (
    select
        -- Portuguese name (join key)
        string_field_0 as category_name_pt,
        -- English translation
        string_field_1 as category_name_en,
        -- Derived: Category group
        case
            when string_field_1 in ('computers', 'computers_accessories', 'electronics', 'tablets_printing_image', 'telephony', 'fixed_telephony')
                then 'Electronics'
            when string_field_1 in ('furniture_decor', 'furniture_living_room', 'furniture_bedroom', 'furniture_mattress_and_upholstery', 'office_furniture', 'home_comfort', 'home_comfort_2', 'home_confort')
                then 'Furniture'
            when string_field_1 in ('health_beauty', 'perfumery', 'diapers_and_hygiene')
                then 'Health & Beauty'
            when string_field_1 in ('sports_leisure', 'toys', 'baby', 'fashion_bags_accessories', 'fashion_shoes', 'fashion_sport', 'fashion_underwear_beach', 'fashio_female_clothing', 'fashion_male_clothing', 'fashion_childrens_clothes')
                then 'Fashion & Lifestyle'
            when string_field_1 in ('housewares', 'kitchen_dining_laundry_garden_furniture', 'garden_tools', 'flowers', 'la_cuisine')
                then 'Home & Garden'
            else 'Other'
        end as category_group
    from source
)
select * from renamed