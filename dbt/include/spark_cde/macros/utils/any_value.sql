{% macro spark_cde__any_value(expression) -%}
    {#-- return any value (non-deterministic)  --#}
    first({{ expression }})

{%- endmacro %}
