{% materialization view, adapter='spark_cde' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
