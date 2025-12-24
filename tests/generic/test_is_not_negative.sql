/*
  GENERIC TEST: is_not_negative
  
  Purpose: Verify column values are >= 0 (allows zero, unlike is_positive)
  
  Usage in schema.yml:
    columns:
      - name: freight_value
        tests:
          - is_not_negative
  
  Returns: Rows where value is negative
*/

{% test is_not_negative(model, column_name) %}

select
    {{ column_name }} as failing_value,
    count(*) as row_count
from {{ model }}
where {{ column_name }} < 0
group by 1

{% endtest %}
