{#- Snowflake-only adaptations for the cloud path (no-ops on duckdb). -#}

{#- Snowpipe appends every export run (a full silver snapshot) to RETAIL.SILVER, so read only the
    latest run: the run id is the parent folder in _EXPORT_FILE (<silver table>/<run_id>/part-…).
    Left-padding makes numeric run ids (github.run_id) compare numerically. -#}
{% macro latest_export(relation) -%}
    {{ return(adapter.dispatch('latest_export')(relation)) }}
{%- endmacro %}

{% macro default__latest_export(relation) -%}
    {{ return(relation) }}
{%- endmacro %}

{% macro snowflake__latest_export(relation) -%}
    {%- set run_id = var('export_run_id', none) -%}
    (
        select * from {{ relation }}
        {%- if run_id %}
        where split_part(_export_file, '/', -2) = '{{ run_id | string | replace("'", "''") }}'
        {%- else %}
        qualify lpad(split_part(_export_file, '/', -2), 64, '0')
            = max(lpad(split_part(_export_file, '/', -2), 64, '0')) over ()
        {%- endif %}
    ) as latest_export
{%- endmacro %}

{#- Source tests (unique keys) must see the same latest run as staging. -#}
{% macro snowflake__get_where_subquery(relation) -%}
    {%- set relation = latest_export(relation) if relation.schema | upper == 'SILVER' else relation -%}
    {%- set where = config.get('where', '') -%}
    {%- if where -%}
        {{ return('(select * from ' ~ relation ~ ' where ' ~ where ~ ') dbt_subquery') }}
    {%- else -%}
        {{ return(relation) }}
    {%- endif -%}
{%- endmacro %}

{#- TRANSFORMER owns only GOLD and can't create schemas: build every model there. -#}
{% macro snowflake__generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
