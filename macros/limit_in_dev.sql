/*
  MACRO: limit_in_dev
  
  Purpose: Limit rows in development for faster iteration
  
  Usage:
    select * from {{ ref('large_table') }}
    {{ limit_in_dev() }}
    
  Behavior:
  - In dev: Adds LIMIT clause
  - In prod: No LIMIT (full data)
*/

{% macro limit_in_dev(n=None) %}
    {%- if n is none -%}
        {%- set n = var('dev_limit', 1000) -%}
    {%- endif -%}
    
    {%- if target.name == 'dev' -%}
        limit {{ n }}
    {%- endif -%}
{% endmacro %}
