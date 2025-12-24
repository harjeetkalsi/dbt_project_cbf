/*
  MACRO: grant_select_to_role
  
  Purpose: Grant SELECT on table to a BigQuery role (for post-hook)
  
  Usage in model:
    {{ config(
      post_hook="{{ grant_select_to_role('bi_readers') }}"
    ) }}
    
  Usage in dbt_project.yml:
    models:
      marts:
        +post_hook: "{{ grant_select_to_role('bi_team') }}"
*/

{% macro grant_select_to_role(role_name) %}
    {%- if target.name == 'prod' -%}
        grant `roles/bigquery.dataViewer` on table {{ this }} to "group:{{ role_name }}@{{ var('domain', 'company.com') }}"
    {%- else -%}
        {# No-op in non-prod environments #}
        select 1
    {%- endif -%}
{% endmacro %}


/*
  MACRO: grant_schema_usage
  
  Purpose: Grant usage on schema (for on-run-end hook)
  
  Usage:
    on-run-end:
      - "{{ grant_schema_usage('analytics_prod', 'bi_team') }}"
*/

{% macro grant_schema_usage(schema_name, role_name) %}
    {%- if target.name == 'prod' -%}
        grant `roles/bigquery.dataViewer` on schema {{ target.project }}.{{ schema_name }} to "group:{{ role_name }}@{{ var('domain', 'company.com') }}"
    {%- else -%}
        select 1
    {%- endif -%}
{% endmacro %}
