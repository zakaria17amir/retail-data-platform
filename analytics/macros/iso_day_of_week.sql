{% macro iso_day_of_week(date_expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(date_expr)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(date_expr) -%}
    extract(isodow from {{ date_expr }})
{%- endmacro %}

{% macro snowflake__iso_day_of_week(date_expr) -%}
    dayofweekiso({{ date_expr }})
{%- endmacro %}
