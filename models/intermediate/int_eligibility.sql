/*
    select health sends full replacement eligibility
    files each week.  Per feedback from the client and analysis of duplicates,
    only enrollment from the latest file should be used for a member.
    The biggest issue this solves is updates to terms dates giving a more accurate
    enrollment.
*/

with max_file_per_member as(
    select
        member_id
        , plan
        , max(ingest_datetime) as max_ingest_datetime
    from {{  ref('stg_eligibility') }}
    group by
        member_id
        , plan
)
,  staged_data as (

    select
        rawe.*
        , row_number() over (partition by rawe.member_id, rawe.enrollment_start_date, rawe.enrollment_end_date
                                order by rawe.ingest_datetime desc) as row_num
    from {{  ref('stg_eligibility') }} rawe
    inner join max_file_per_member m
        on rawe.member_id = m.member_id
        and rawe.ingest_datetime = m.max_ingest_datetime
        and rawe.plan = m.plan
)

, get_first_ingest_datetime as (
    select
        member_id
        , plan
        , min(ingest_datetime) as first_ingest_datetime
    from staged_data
    group by member_id, plan
)

/* source fields not mapped are commented out */
, staged_data_deduped as (

    select
        group_number
        /*, group_division*/
        , group_division_name
        /*, class_id*/
        /*, class_description*/
        , subscriber_id
        , sd.member_id
        , subscriber_relation
        , gender
        , birth_date
        , enrollment_start_date
        , enrollment_end_date
        /*, coverage_type*/
        /*, plan_type*/
        , sd.plan
        /*, prod_desc*/
        , first_name
        , last_name
        , member_mi
        , zip_code
        , social_security_number
        /*, subscriber_ssn*/
        , data_source
        , file_name
        , file_date
        , gfid.first_ingest_datetime as ingest_datetime
    from staged_data as sd
    left join get_first_ingest_datetime as gfid
    on sd.member_id = gfid.member_id
    and sd.plan = gfid.plan
    where row_num = 1

)

, mapping as (

    select
          member_id as person_id
        , member_id as member_id
        , subscriber_id
        , case
            when lower(gender) = 'm' then 'male'
            when lower(gender) = 'f' then 'female'
            else 'unknown'
          end as gender
        , null as race
        , null as ethnicity
        , date(birth_date) as birth_date
        , null as death_date
        , 0 as death_flag
        , date(enrollment_start_date) as enrollment_start_date
          /* map null and future enrollment end dates to current month */
        , case
            when date(enrollment_end_date) >= current_date
            then last_day(current_date, 'year')
            when enrollment_end_date is null
            then last_day(current_date, 'year')
            else date(enrollment_end_date)
          end as enrollment_end_date
        /*
            manually mapping to string for payer instead of using
            group_division_name to account for "Medicity" records
        */
        , 'Health Catalyst' as payer
        , 'self-insured' as payer_type
        , plan
        , null as original_reason_entitlement_code
        , null as dual_status_code
        , null as medicare_status_code
        , group_number as group_id
        , group_division_name as group_name
        , first_name
        , member_mi as middle_name
        , last_name
        , null as name_suffix
        , social_security_number
        , subscriber_relation
        , null as address
        , null as city
        , null as state
        , zip_code
        , null as phone
        , null as email
        , data_source
        , file_name
        , file_date
        , ingest_datetime
    from staged_data_deduped

)

, data_types as (

    select
          cast(person_id as TEXT) as person_id
        , cast(member_id as TEXT) as member_id
        , cast(subscriber_id as TEXT) as subscriber_id
        , cast(gender as TEXT) as gender
        , cast(race as TEXT) as race
        , cast(ethnicity as TEXT) as ethnicity
        , cast(birth_date as date) as birth_date
        , cast(death_date as date) as death_date
        , cast(death_flag as integer) as death_flag
        , cast(enrollment_start_date as date) as enrollment_start_date
        , cast(enrollment_end_date as date) as enrollment_end_date
        , cast(payer as TEXT) as payer
        , cast(payer_type as TEXT) as payer_type
        , cast(plan as TEXT) as plan
        , cast(original_reason_entitlement_code as TEXT) as original_reason_entitlement_code
        , cast(dual_status_code as TEXT) as dual_status_code
        , cast(medicare_status_code as TEXT) as medicare_status_code
        , cast(group_id as TEXT) as group_id
        , cast(group_name as TEXT) as group_name
        , cast(first_name as TEXT) as first_name
        , cast(middle_name as TEXT) as middle_name
        , cast(last_name as TEXT) as last_name
        , cast(name_suffix as TEXT) as name_suffix
        , cast(social_security_number as TEXT) as social_security_number
        , cast(subscriber_relation as TEXT) as subscriber_relation
        , cast(address as TEXT) as address
        , cast(city as TEXT) as city
        , cast(state as TEXT) as state
        , cast(zip_code as TEXT) as zip_code
        , cast(phone as TEXT) as phone
        , cast(email as TEXT) as email
        , cast(data_source as TEXT) as data_source
        , cast(file_name as TEXT) as file_name
        , cast(file_date as date) as file_date
        , cast(ingest_datetime as datetime) as ingest_datetime
    from mapping

)

select
      person_id
    , member_id
    , subscriber_id
    , gender
    , race
    , ethnicity
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , plan
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , group_id
    , group_name
    , first_name
    , middle_name
    , last_name
    , name_suffix
    , social_security_number
    , subscriber_relation
    , address
    , city
    , state
    , zip_code
    , phone
    , email
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from data_types
