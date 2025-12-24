/*
  GENERIC TEST: is_positive
  
  Purpose: Verify column values are strictly positive (> 0)
  
  Usage in schema.yml:
    columns:
      - name: order_total
        tests:
          - is_positive
  
  Returns: Rows where value is NOT positive (test fails if rows returned)
*/

{% test is_positive(model, column_name) %}

select
    {{ column_name }} as failing_value,
    count(*) as row_count
from {{ model }}
where {{ column_name }} <= 0
   or {{ column_name }} is null
group by 1

{% endtest %}
