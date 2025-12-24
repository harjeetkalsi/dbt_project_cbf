/*
  MACRO: log_run_start
  
  Purpose: Log when dbt run starts (for on-run-start hook)
  
  Usage in dbt_project.yml:
    on-run-start:
      - "{{ log_run_start() }}"
*/

{% macro log_run_start() %}
    {{ log("=" * 60, info=true) }}
    {{ log("DBT RUN STARTED", info=true) }}
    {{ log("Target: " ~ target.name, info=true) }}
    {{ log("Project: " ~ project_name, info=true) }}
    {{ log("Time: " ~ modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), info=true) }}
    {{ log("=" * 60, info=true) }}
    
    {# Return empty string since this is used in hook #}
    {{ return('') }}
{% endmacro %}


/*
  MACRO: log_run_end
  
  Purpose: Log summary when dbt run completes (for on-run-end hook)
  
  Usage in dbt_project.yml:
    on-run-end:
      - "{{ log_run_end(results) }}"
*/

{% macro log_run_end(results) %}
    {% set success_count = results | selectattr('status', 'equalto', 'success') | list | length %}
    {% set error_count = results | selectattr('status', 'equalto', 'error') | list | length %}
    {% set skip_count = results | selectattr('status', 'equalto', 'skipped') | list | length %}
    
    {{ log("=" * 60, info=true) }}
    {{ log("DBT RUN COMPLETED", info=true) }}
    {{ log("Success: " ~ success_count, info=true) }}
    {{ log("Errors: " ~ error_count, info=true) }}
    {{ log("Skipped: " ~ skip_count, info=true) }}
    {{ log("Total: " ~ results | length, info=true) }}
    {{ log("=" * 60, info=true) }}
    
    {% if error_count > 0 %}
        {{ log("FAILED MODELS:", info=true) }}
        {% for result in results if result.status == 'error' %}
            {{ log("  - " ~ result.node.name ~ ": " ~ result.message, info=true) }}
        {% endfor %}
    {% endif %}
    
    {{ return('') }}
{% endmacro %}
