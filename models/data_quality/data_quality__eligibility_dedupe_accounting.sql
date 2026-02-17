with source as (

    select
          data_source
        , member_id
    from {{ ref('stg_eligibility') }}

)

, mapped as (

    select
          data_source
        , member_id
    from {{ ref('eligibility') }}

)

, source_records as (

    select
          data_source
        , count(*) as count
    from source
    group by data_source

)

, source_patients as (

    select
          data_source
        , count(distinct member_id) as count_distinct
    from source
    group by data_source

)

, mapped_records as (

    select
          data_source
        , count(*) as count
    from mapped
    group by data_source

)

, mapped_patients as (

    select
          data_source
        , count(distinct member_id) as count_distinct
    from mapped
    group by data_source

)

, final_cte as (

    select
          'eligibility' as source_type
        , 1 as step
        , 'Start (raw)' as description
        , source_records.data_source
        , source_records.count as records
        , source_patients.count_distinct as patients
    from source_records
        left join source_patients
            on source_records.data_source = source_patients.data_source

    union all

    select
          'eligibility' as source_type
        , 2 as step
        , 'DISTINCT on all rows' as description
        , mapped_records.data_source
        , mapped_records.count as records
        , mapped_patients.count_distinct as patients
    from mapped_records
        left join mapped_patients
            on mapped_records.data_source = mapped_patients.data_source

)

select * from final_cte