{% macro null_negative_and_zero_values(value) %}
    CASE 
        WHEN {{ value }} <= 0 THEN NULL
        ELSE {{ value }}
    END
{% endmacro %}
