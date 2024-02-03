{% macro rolling_aggregation_n_periods(column_name, partition_by, order_by='created_at', aggregation='avg', periods=7) %}
    {{ aggregation }}( {{ column_name }} ) over (
                partition by {{ partition_by }}
                order by {{ order_by }}
                rows between {{ periods }} - 1 preceding and current row
            ) as {{ aggregation }}_{{ periods }}_periods_{{ column_name }}
{% endmacro %}
