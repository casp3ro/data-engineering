{% macro delta_scan_from_path(path_key) %}
  SELECT * FROM delta_scan('{{ get_delta_path(path_key) }}')
{% endmacro %}
