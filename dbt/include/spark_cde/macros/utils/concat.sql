{% macro spark_cde__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
