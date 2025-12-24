/*
  MACRO: generate_date_parts
  
  Purpose: Generate common date dimension columns from a date field
  
  Usage:
    select
      order_id,
      {{ generate_date_parts('order_date') }}
    from orders
    
  Output columns:
  - [prefix]_year
  - [prefix]_month
  - [prefix]_quarter
  - [prefix]_week
  - [prefix]_day_of_week
  - is_weekend
*/

{% macro generate_date_parts(date_column, prefix=None) %}
    {%- if prefix is none -%}
        {%- set prefix = date_column.split('.')[-1].replace('_date', '').replace('_at', '') -%}
    {%- endif -%}
    
    extract(year from {{ date_column }}) as {{ prefix }}_year,
    extract(month from {{ date_column }}) as {{ prefix }}_month,
    extract(quarter from {{ date_column }}) as {{ prefix }}_quarter,
    extract(week from {{ date_column }}) as {{ prefix }}_week,
    extract(dayofweek from {{ date_column }}) as {{ prefix }}_day_of_week,
    case 
        when extract(dayofweek from {{ date_column }}) in (1, 7) then true 
        else false 
    end as is_weekend
{% endmacro %}
