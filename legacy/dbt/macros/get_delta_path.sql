{% macro get_delta_path(path_key) %}
  {% set mode = var('pipeline_mode', 'local') %}
  {% set paths = var('delta_paths') %}
  {% if mode == 'databricks' %}
    {{ return(paths['databricks'][path_key]) }}
  {% else %}
    {{ return(paths['local'][path_key]) }}
  {% endif %}
{% endmacro %}
