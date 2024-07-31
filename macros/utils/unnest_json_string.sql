{% macro unnest_json_string(column_name) -%}
cast(json_parse({{ column_name }}) AS ARRAY<JSON>)
{%- endmacro %}