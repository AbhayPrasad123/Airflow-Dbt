{% macro calc_margin(profit, net_revenue) %}

CASE 
    WHEN {{ net_revenue }} = 0 THEN 0
    ELSE {{ profit }} / {{ net_revenue }}
END

{% endmacro %}