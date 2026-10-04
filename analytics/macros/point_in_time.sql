{#- SCD2 join condition: fact_ts in [valid_from, valid_to); a key's first version also covers
    earlier facts so CDC-born keys (valid_from = change time) never drop rows. -#}
{% macro point_in_time(dim, fact_ts) -%}
    ({{ dim }}.is_first_version or {{ fact_ts }} >= {{ dim }}.valid_from)
    and ({{ dim }}.valid_to is null or {{ fact_ts }} < {{ dim }}.valid_to)
{%- endmacro %}
