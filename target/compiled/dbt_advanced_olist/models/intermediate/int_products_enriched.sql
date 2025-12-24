

/*
  INTERMEDIATE MODEL: int_products_enriched
  
  Purpose: Enrich products with category translations and sales metrics
*/

with products as (
    select * from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_products`
),

categories as (
    select * from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_product_categories`
),

order_items as (
    select * from `big-query-dbt-481111`.`dbt_dev_yourname_staging`.`stg_order_items`
),

-- Calculate product-level sales metrics
product_sales as (
    select
        product_id,
        count(distinct order_id) as times_ordered,
        sum(item_price) as total_revenue,
        avg(item_price) as avg_selling_price,
        min(item_price) as min_selling_price,
        max(item_price) as max_selling_price
    from order_items
    group by 1
),

-- Join products with categories and sales
enriched as (
    select
        -- Product info
        p.product_id,
        p.category_name_pt,
        coalesce(c.category_name_en, 'Unknown') as category_name,
        coalesce(c.category_group, 'Other') as category_group,
        
        -- Dimensions
        p.weight_grams,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.volume_cm3,
        p.size_category,
        
        -- Product listing quality
        p.name_length,
        p.description_length,
        p.photo_count,
        
        -- Derived: Listing quality score (0-100)
        (
            case when p.name_length > 0 then 20 else 0 end +
            case when p.description_length > 100 then 30 else p.description_length * 0.3 end +
            case when p.photo_count >= 3 then 50 else p.photo_count * 15 end
        ) as listing_quality_score,
        
        -- Sales metrics
        coalesce(ps.times_ordered, 0) as times_ordered,
        coalesce(ps.total_revenue, 0) as total_revenue,
        coalesce(ps.avg_selling_price, 0) as avg_selling_price,
        
        -- Derived: Product popularity tier
        case
            when coalesce(ps.times_ordered, 0) = 0 then 'Never Sold'
            when ps.times_ordered < 5 then 'Low'
            when ps.times_ordered < 20 then 'Medium'
            when ps.times_ordered < 50 then 'High'
            else 'Top Seller'
        end as popularity_tier

    from products p
    left join categories c on p.category_name_pt = c.category_name_pt
    left join product_sales ps on p.product_id = ps.product_id
)

select * from enriched