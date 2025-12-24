/*
  GENERIC TEST: row_count_delta
  
  Purpose: Verify row count hasn't changed dramatically compared to another model
  
  Usage in schema.yml:
    models:
      - name: fct_orders
        tests:
          - row_count_delta:
              compare_model: ref('fct_orders_previous')
              max_delta_percent: 10
  
  Parameters:
    - model: The current model
    - compare_model: Model to compare against
    - max_delta_percent: Maximum allowed change (default: 10%)
  
  Returns: Row if delta exceeds threshold
*/

{% test row_count_delta(model, compare_model, max_delta_percent=10) %}

with current_count as (
    select count(*) as cnt from {{ model }}
),

compare_count as (
    select count(*) as cnt from {{ compare_model }}
),

delta as (
    select
        c.cnt as current_rows,
        p.cnt as compare_rows,
        case 
            when p.cnt = 0 then 100.0
            else abs(c.cnt - p.cnt) * 100.0 / p.cnt
        end as delta_percent
    from current_count c
    cross join compare_count p
)

select
    current_rows,
    compare_rows,
    round(delta_percent, 2) as delta_percent,
    {{ max_delta_percent }} as max_allowed_percent
from delta
where delta_percent > {{ max_delta_percent }}

{% endtest %}
