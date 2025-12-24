/*
  MACRO: safe_divide
  
  Purpose: Division that handles zero denominator and nulls
  
  Usage:
    select
      {{ safe_divide('revenue', 'orders') }} as avg_order_value,
      {{ safe_divide('completed', 'total', default=0, precision=2) }} as completion_rate
      
  Parameters:
  - numerator: Column or expression for numerator
  - denominator: Column or expression for denominator  
  - default: Value when denominator is 0 or null (default: null)
  - precision: Decimal places to round to (default: none)
*/

{% macro safe_divide(numerator, denominator, default=none, precision=none) %}
    {%- if precision is not none -%}
        round(
    {%- endif -%}
    
    case
        when {{ denominator }} = 0 then {{ default if default is not none else 'null' }}
        when {{ denominator }} is null then {{ default if default is not none else 'null' }}
        else cast({{ numerator }} as float64) / cast({{ denominator }} as float64)
    end
    
    {%- if precision is not none -%}
        , {{ precision }})
    {%- endif -%}
{% endmacro %}
