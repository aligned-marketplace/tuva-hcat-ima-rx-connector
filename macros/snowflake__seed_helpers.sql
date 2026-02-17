{#
    These macros add adapter-specific logic for creating and refreshing seeds
    instead of using the default macros from dbt. This is required for
    maintaining seeds shared in a Snowflake data share.

    When copy_grants is enabled in dbt_project.yml, this macro will include the
    syntax for maintaining grants previously added to tables being shared. This
    logic is similar to how the dbt-snowflake adapter creates tables for models.

    References:
    https://github.com/dbt-labs/dbt-adapters/blob/main/dbt/include/global_project/macros/materializations/seeds/helpers.sql
    https://github.com/dbt-labs/dbt-snowflake/blob/main/dbt/include/snowflake/macros/relations/table/create.sql
#}

{% macro snowflake__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set copy_grants = model['config'].get('copy_grants') -%}

  {% set sql %}
    create or replace table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    ) {% if copy_grants and not temporary -%} copy grants {%- endif %}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{% macro snowflake__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    {% if full_refresh %}
        {% set sql = snowflake__create_csv_table(model, agate_table) %}
    {% else %}
        {{ adapter.truncate_relation(old_relation) }}
        {% set sql = "truncate table " ~ old_relation %}
    {% endif %}

    {{ return(sql) }}
{% endmacro %}