{% macro generate_date_columns(date_column, prefix='date') %}

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