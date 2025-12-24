/*
  GENERIC TEST: is_valid_email
  
  Purpose: Verify column contains valid email format
  
  Usage in schema.yml:
    columns:
      - name: email
        tests:
          - is_valid_email
  
  Pattern: Basic email regex validation
  
  Returns: Rows with invalid email format
*/

{% test is_valid_email(model, column_name) %}

select
    {{ column_name }} as invalid_email,
    count(*) as row_count
from {{ model }}
where {{ column_name }} is not null
  and not regexp_contains(
    {{ column_name }},
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
  )
group by 1

{% endtest %}
