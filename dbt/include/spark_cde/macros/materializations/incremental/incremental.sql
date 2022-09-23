{% materialization incremental, adapter='spark_cde' -%}

  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='append') -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% do target_relation.log_relation(raw_strategy) %}

  {% if strategy == 'insert_overwrite' and partition_by %}
    {% call statement() %}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {% endcall %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% if not is_delta %} {#-- If Delta, we will `create or replace` below, so no need to drop --#}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do adapter.drop_relation(tmp_relation.incorporate(type='table')) %}
    {% do adapter.drop_relation(tmp_relation.incorporate(type='view')) %}
    {% do run_query(create_table_as(False, tmp_relation, sql)) %}
    {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% set build_sql = dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% do adapter.drop_relation(tmp_relation.incorporate(type='table')) %}
  {% do adapter.drop_relation(tmp_relation.incorporate(type='view')) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
