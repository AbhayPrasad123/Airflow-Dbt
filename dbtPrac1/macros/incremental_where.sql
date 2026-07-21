{% macro incremental_where(cdc_column) %}
  {% if is_incremental() %}
    where {{ cdc_column }} >
          (select max({{ cdc_column }}) from {{ this }})
  {% endif %}
{% endmacro %}
