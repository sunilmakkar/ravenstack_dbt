with src as (

    select *
    from {{ ref('ravenstack_feature_usage') }}

),

renamed as (

    select
        cast(usage_id as varchar)           as usage_id,
        cast(subscription_id as varchar)   as subscription_id,

        try_to_date(usage_date)            as usage_date,

        trim(feature_name)                 as feature_name,

        try_to_number(usage_count)         as usage_count,
        try_to_number(usage_duration_secs) as usage_duration_secs,
        try_to_number(error_count)         as error_count,

        try_to_boolean(is_beta_feature)    as is_beta_feature

    from src

)

select *
from renamed
qualify row_number() over (
    partition by usage_id
    order by usage_date desc, subscription_id
) = 1
