with src as (

    select *
    from {{ ref('ravenstack_support_tickets') }}

),

renamed as (

    select
        cast(ticket_id as varchar)              as ticket_id,
        cast(account_id as varchar)             as account_id,

        cast(submitted_at as timestamp_ntz) as submitted_at,
        cast(closed_at    as timestamp_ntz) as closed_at,

        cast(resolution_time_hours as number(38,2))        as resolution_time_hours,

        lower(trim(priority))                   as priority,

        cast(first_response_time_minutes as number(38,0))  as first_response_time_minutes,

        cast(satisfaction_score as number(38,0))           as satisfaction_score,
        try_to_boolean(escalation_flag)         as escalation_flag

    from src

)

select *
from renamed
