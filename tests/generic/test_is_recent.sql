/*
  GENERIC TEST: is_recent
  
  Purpose: Verify date/timestamp column has values within N days of today
  
  Usage in schema.yml:
    columns:
      - name: last_updated_at
        tests:
          - is_recent:
              days: 7
  
  Parameters:
    - model: The model to test
    - column_name: Date/timestamp column
    - days: Maximum age in days (default: 7)
  
  Returns: Rows where date is older than N days
*/

{% test is_recent(model, column_name, days=7) %}

select
    {{ column_name }} as old_date,
    date_diff(current_date(), date({{ column_name }}), day) as days_old,
    count(*) as row_count
from {{ model }}
where {{ column_name }} is not null
  and date_diff(current_date(), date({{ column_name }}), day) > {{ days }}
group by 1, 2

{% endtest %}
