/*
  MACRO: mode
  Purpose: Calculate the statistical mode (most frequent value) in a GROUP BY
  Usage:
    select
      customer_id,
      {{ mode('product_category') }} as favorite_category
    from orders
    group by customer_id
  Note: BigQuery specific - uses APPROX_TOP_COUNT
*/
{% macro mode(column_name) %}
    (select x.value from unnest(approx_top_count({{ column_name }}, 1)) as x limit 1)
{% endmacro %}

/*
  MACRO: mode_with_count
  Purpose: Get mode and its frequency
  Usage:
    select
      customer_id,
      {{ mode_with_count('category') }}.value as top_category,
      {{ mode_with_count('category') }}.count as category_count
    from orders
    group by customer_id
*/
{% macro mode_with_count(column_name) %}
    (select as struct x.value, x.count from unnest(approx_top_count({{ column_name }}, 1)) as x limit 1)
{% endmacro %}