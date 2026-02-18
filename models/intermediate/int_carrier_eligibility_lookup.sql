-- Union Regence and Select Health eligibility, then deduplicate per IMA
-- individual_id to arrive at the latest person_id and plan for each member.

with regence_elig as (

    select
          person_id
        , first_name
        , last_name
        , birth_date
        , payer
        , plan
        , data_source
        , enrollment_start_date
        , enrollment_end_date
        , file_date
        , ingest_datetime
    from {{ source('tuva_regence', 'eligibility') }}

)

, select_health_elig as (

    select
          person_id
        , first_name
        , last_name
        , birth_date
        , payer
        , plan
        , data_source
        , enrollment_start_date
        , enrollment_end_date
        , file_date
        , ingest_datetime
    from {{ source('tuva_select_health', 'eligibility') }}

)

, unioned as (

    select * from regence_elig
    union all
    select * from select_health_elig

)

-- Join to IMA staging to get individual_id, then pick the latest
-- eligibility record per individual_id
, ima_members as (

    select distinct
          member_id as individual_id
        , first_name
        , last_name
        , date_of_birth
    from {{ ref('stg_pharmacy_claim') }}

)

, joined as (

    select
          ima.individual_id
        , elig.person_id
        , elig.payer
        , elig.plan
        , elig.data_source       as carrier_data_source
        , elig.enrollment_start_date
        , elig.enrollment_end_date
        , elig.file_date
        , elig.ingest_datetime
        , row_number() over (
            partition by ima.individual_id
            order by elig.file_date desc nulls last
                   , elig.ingest_datetime desc nulls last
                   , elig.enrollment_end_date desc nulls last
          ) as row_num
    from ima_members as ima
    inner join unioned as elig
        on upper(trim(ima.first_name)) = upper(trim(elig.first_name))
        and upper(trim(ima.last_name)) = upper(trim(elig.last_name))
        and try_cast(ima.date_of_birth as date) = elig.birth_date

)

select
      individual_id
    , person_id
    , payer
    , plan
    , carrier_data_source
from joined
where row_num = 1
