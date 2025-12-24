/*
  MACRO: cents_to_currency
  
  Purpose: Convert cents to currency with proper formatting
  
  Usage:
    select
      {{ cents_to_currency('price_cents') }} as price,
      {{ cents_to_currency('amount_cents', precision=4) }} as amount
      
  Parameters:
  - column_name: Column containing cents value
  - precision: Decimal places (default: 2)
*/

{% macro cents_to_currency(column_name, precision=2) %}
    round(cast({{ column_name }} as float64) / 100, {{ precision }})
{% endmacro %}


/*
  MACRO: format_currency
  
  Purpose: Format number as currency string (for display)
  
  Usage:
    select {{ format_currency('total', 'BRL') }} as formatted_total
    
  Note: Returns string, use only for display/exports
*/

{% macro format_currency(column_name, currency_code='BRL') %}
    concat('{{ currency_code }} ', format('%,.2f', cast({{ column_name }} as float64)))
{% endmacro %}
