{% macro tenure_bucket(tenure_days) -%}
    case
        when {{ tenure_days }} is null then null
        when {{ tenure_days }} <= 30 then '0–30'
        when {{ tenure_days }} <= 90 then '31–90'
        when {{ tenure_days }} <= 180 then '91–180'
        when {{ tenure_days }} <= 365 then '181–365'
        else '365+'
    end
{%- endmacro %}
