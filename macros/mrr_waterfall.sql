{% macro mrr_waterfall(curr_mrr, prior_mrr) -%}
    case
        when {{ prior_mrr }} = 0 and {{ curr_mrr }} > 0 then 'new'
        when {{ prior_mrr }} > 0 and {{ curr_mrr }} > {{ prior_mrr }} then 'expansion'
        when {{ prior_mrr }} > 0 and {{ curr_mrr }} < {{ prior_mrr }} and {{ curr_mrr }} > 0 then 'contraction'
        when {{ prior_mrr }} > 0 and {{ curr_mrr }} = 0 then 'churned'
        else 'none'
    end
{%- endmacro %}
