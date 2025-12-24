/*
  ADAPTER MACROS: Cross-Database Compatibility
  
  Purpose: Write SQL that works across BigQuery, Snowflake, Redshift, etc.
  
  Reference: Slide 17 - Adapter Macros
  
  target.type values: bigquery, snowflake, redshift, postgres, databricks
*/

-- ============================================================
-- DATE_TRUNC: Different syntax per database
-- ============================================================

{% macro date_trunc_custom(datepart, date_column) %}
    {% if target.type == 'bigquery' %}
        date_trunc({{ date_column }}, {{ datepart }})
    {% elif target.type == 'snowflake' %}
        date_trunc('{{ datepart }}', {{ date_column }})
    {% elif target.type == 'redshift' %}
        date_trunc('{{ datepart }}', {{ date_column }})
    {% elif target.type == 'postgres' %}
        date_trunc('{{ datepart }}', {{ date_column }})
    {% else %}
        date_trunc('{{ datepart }}', {{ date_column }})
    {% endif %}
{% endmacro %}


-- ============================================================
-- CURRENT_TIMESTAMP: Cross-database compatible
-- ============================================================

{% macro current_timestamp_custom() %}
    {% if target.type == 'bigquery' %}
        current_timestamp()
    {% elif target.type == 'snowflake' %}
        current_timestamp()
    {% elif target.type == 'redshift' %}
        getdate()
    {% elif target.type == 'postgres' %}
        now()
    {% else %}
        current_timestamp
    {% endif %}
{% endmacro %}


-- ============================================================
-- STRING_AGG / LISTAGG: Concatenate strings in group by
-- ============================================================

{% macro string_agg_custom(column, delimiter=',', order_by=None) %}
    {% if target.type == 'bigquery' %}
        string_agg({{ column }}, '{{ delimiter }}' {% if order_by %}order by {{ order_by }}{% endif %})
    {% elif target.type == 'snowflake' %}
        listagg({{ column }}, '{{ delimiter }}') {% if order_by %}within group (order by {{ order_by }}){% endif %}
    {% elif target.type == 'redshift' %}
        listagg({{ column }}, '{{ delimiter }}') {% if order_by %}within group (order by {{ order_by }}){% endif %}
    {% elif target.type == 'postgres' %}
        string_agg({{ column }}::text, '{{ delimiter }}' {% if order_by %}order by {{ order_by }}{% endif %})
    {% else %}
        string_agg({{ column }}, '{{ delimiter }}')
    {% endif %}
{% endmacro %}


-- ============================================================
-- HASH: Generate hash for surrogate keys
-- ============================================================

{% macro hash_custom(columns) %}
    {% if target.type == 'bigquery' %}
        to_hex(md5(concat({% for col in columns %}cast({{ col }} as string){% if not loop.last %}, '|', {% endif %}{% endfor %})))
    {% elif target.type == 'snowflake' %}
        md5(concat({% for col in columns %}coalesce(cast({{ col }} as varchar), ''){% if not loop.last %}, '|', {% endif %}{% endfor %}))
    {% elif target.type == 'redshift' %}
        md5({% for col in columns %}coalesce(cast({{ col }} as varchar), ''){% if not loop.last %} || '|' || {% endif %}{% endfor %})
    {% elif target.type == 'postgres' %}
        md5({% for col in columns %}coalesce(cast({{ col }} as varchar), ''){% if not loop.last %} || '|' || {% endif %}{% endfor %})
    {% else %}
        md5(concat({% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}))
    {% endif %}
{% endmacro %}


-- ============================================================
-- DATEDIFF: Calculate difference between dates
-- ============================================================

{% macro datediff_custom(datepart, start_date, end_date) %}
    {% if target.type == 'bigquery' %}
        date_diff({{ end_date }}, {{ start_date }}, {{ datepart }})
    {% elif target.type == 'snowflake' %}
        datediff({{ datepart }}, {{ start_date }}, {{ end_date }})
    {% elif target.type == 'redshift' %}
        datediff({{ datepart }}, {{ start_date }}, {{ end_date }})
    {% elif target.type == 'postgres' %}
        extract({{ datepart }} from age({{ end_date }}, {{ start_date }}))
    {% else %}
        datediff({{ datepart }}, {{ start_date }}, {{ end_date }})
    {% endif %}
{% endmacro %}


-- ============================================================
-- NOTE: For most common cases, use dbt_utils macros instead:
--   - dbt_utils.current_timestamp()
--   - dbt_utils.dateadd()
--   - dbt_utils.datediff()
--   - dbt_utils.date_trunc()
-- These are already cross-database compatible!
-- ============================================================
