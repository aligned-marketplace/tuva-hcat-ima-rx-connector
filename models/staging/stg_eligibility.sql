with source_data as (

    select
          _file
        , _line
        , _modified
        , _fivetran_synced
        , employee_id
        , member_ssn
        , prod_desc
        , group_number
        , prod_name
        , class_id
        , member_zip
        , member_last_name
        , member_termination_date
        , member_gender
        , member_first_name
        , class_description
        , group_division_name
        , member_relationship
        , subscriber_ssn
        , group_division
        , member_mi
        , member_id
        , member_effective_date
        , plan_type
        , member_dob
        , coverage_type
    from {{ source('select_health','eligibility') }}
)

, mapping as (

    select
          group_number
        , group_division
        , group_division_name
        , class_id
        , class_description
        , employee_id as subscriber_id
        , member_id as member_id
        , member_relationship as subscriber_relation
        , member_gender as gender
        , member_dob as birth_date
        , member_effective_date as enrollment_start_date
        , member_termination_date as enrollment_end_date
        , coverage_type
        , plan_type
        , prod_name as plan
        , prod_desc
        , member_first_name as first_name
        , member_last_name as last_name
        , member_mi
        , member_zip as zip_code
        , member_ssn as social_security_number
        , subscriber_ssn
        , 'Select Health' as data_source
        , _FILE as file_name
        , CASE
            -- Try full timestamp format (YYYYMMDDHHMMSS)
            WHEN TRY_TO_TIMESTAMP(REGEXP_SUBSTR(file_name, '[0-9]{14}'), 'YYYYMMDDHHMISS') IS NOT NULL
                THEN TRY_TO_TIMESTAMP(REGEXP_SUBSTR(file_name, '[0-9]{14}'), 'YYYYMMDDHHMISS')

            -- Try date format (YYYYMMDD)
            WHEN TRY_TO_DATE(REGEXP_SUBSTR(file_name, '[0-9]{8}'), 'YYYYMMDD') IS NOT NULL
                THEN TRY_TO_DATE(REGEXP_SUBSTR(file_name, '[0-9]{8}'), 'YYYYMMDD')

            -- Try year-month format (YYYYMM) and convert to first day of month
            WHEN TRY_TO_DATE(REGEXP_SUBSTR(file_name, '[0-9]{6}') || '01', 'YYYYMMDD') IS NOT NULL
                THEN TRY_TO_DATE(REGEXP_SUBSTR(file_name, '[0-9]{6}') || '01', 'YYYYMMDD')

            ELSE NULL
          END as file_date
        , _FIVETRAN_SYNCED as ingest_datetime
    from source_data

)

, data_types as (

    select
          cast(group_number as {{ dbt.type_string() }}) as group_number
        , cast(group_division as {{ dbt.type_string() }}) as group_division
        , cast(group_division_name as {{ dbt.type_string() }}) as group_division_name
        , cast(class_id as {{ dbt.type_string() }}) as class_id
        , cast(class_description as {{ dbt.type_string() }}) as class_description
        , cast(subscriber_id as {{ dbt.type_string() }}) as subscriber_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(subscriber_relation as {{ dbt.type_string() }}) as subscriber_relation
        , cast(gender as {{ dbt.type_string() }}) as gender
        , cast(birth_date as {{ dbt.type_string() }}) as birth_date
        , cast(enrollment_start_date as {{ dbt.type_string() }}) as enrollment_start_date
        , cast(enrollment_end_date as {{ dbt.type_string() }}) as enrollment_end_date
        , cast(coverage_type as {{ dbt.type_string() }}) as coverage_type
        , cast(plan_type as {{ dbt.type_string() }}) as plan_type
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(prod_desc as {{ dbt.type_string() }}) as prod_desc
        , cast(first_name as {{ dbt.type_string() }}) as first_name
        , cast(last_name as {{ dbt.type_string() }}) as last_name
        , cast(member_mi as {{ dbt.type_string() }}) as member_mi
        , cast(zip_code as {{ dbt.type_string() }}) as zip_code
        , cast(social_security_number as {{ dbt.type_string() }}) as social_security_number
        , cast(subscriber_ssn as {{ dbt.type_string() }}) as subscriber_ssn
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(file_date as date) as file_date
        , cast(ingest_datetime as datetime) as ingest_datetime
    from mapping

)

select
      group_number
    , group_division
    , group_division_name
    , class_id
    , class_description
    , subscriber_id
    , member_id
    , subscriber_relation
    , gender
    , birth_date
    , enrollment_start_date
    , enrollment_end_date
    , coverage_type
    , plan_type
    , plan
    , prod_desc
    , first_name
    , last_name
    , member_mi
    , zip_code
    , social_security_number
    , subscriber_ssn
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from data_types

union all

select
      group_number
    , group_division
    , group_division_name
    , class_id
    , class_description
    , subscriber_id
    , member_id
    , subscriber_relation
    , gender
    , birth_date
    , enrollment_start_date
    , enrollment_end_date
    , coverage_type
    , plan_type
    , plan
    , prod_desc
    , first_name
    , last_name
    , member_mi
    , zip_code
    , social_security_number
    , subscriber_ssn
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from {{ source('tuva_historical_select_health','eligibility') }}
