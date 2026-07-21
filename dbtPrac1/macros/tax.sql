{% macro add_tax(col) %}
    {{ col }} * 0.1
{% endmacro %}