with source as (

    select
          data_source
        , claim_id
        , member_id
    from  {{ ref('stg_pharmacy_claim') }}

)

, adjustments as (

    select
          data_source
        , claim_id
        , member_id
    from {{ ref('int_pharmacy_claim_adr') }}

)

, deduped as (

    select
          data_source
        , claim_id
        , member_id
    from {{ ref('pharmacy_claim') }}

)

, source_records as (

    select
          data_source
        , count(*) as count
    from source
    group by data_source

)

, source_claims as (

    select
          data_source
        , count(distinct claim_id) as count_distinct
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

, adjustments_records as (

    select
          data_source
        , count(*) as count
    from adjustments
    group by data_source

)

, adjustments_claims as (

    select
          data_source
        , count(distinct claim_id) as count_distinct
    from adjustments
    group by data_source

)

, adjustments_patients as (

    select
          data_source
        , count(distinct member_id) as count_distinct
    from adjustments
    group by data_source

)

, deduped_records as (

    select
          data_source
        , count(*) as count
    from deduped
    group by data_source

)

, deduped_claims as (

    select
          data_source
        , count(distinct claim_id) as count_distinct
    from deduped
    group by data_source

)

, deduped_patients as (

    select
          data_source
        , count(distinct member_id) as count_distinct
    from deduped
    group by data_source

)

, final_cte as (

    select
          'pharmacy_claim' as source_type
        , 1 as step
        , 'Start (raw)' as description
        , source_records.data_source
        , source_records.count as records
        , source_claims.count_distinct as claims
        , source_patients.count_distinct as patients
    from source_records
        left join source_claims
            on source_records.data_source = source_claims.data_source
        left join source_patients
            on source_records.data_source = source_patients.data_source

    union all

    select
          'pharmacy_claim' as source_type
        , 2 as step
        , 'Identify adjustments and reversals' as description
        , adjustments_records.data_source
        , adjustments_records.count as records
        , adjustments_claims.count_distinct as claims
        , adjustments_patients.count_distinct as patients
    from adjustments_records
        left join adjustments_claims
            on adjustments_records.data_source = adjustments_claims.data_source
        left join adjustments_patients
            on adjustments_records.data_source = adjustments_patients.data_source

    union all

    select
          'pharmacy_claim' as source_type
        , 3 as step
        , 'Dedupe on claim_id and claim_line_number, and remove reversals' as description
        , deduped_records.data_source
        , deduped_records.count as records
        , deduped_claims.count_distinct as claims
        , deduped_patients.count_distinct as patients
    from deduped_records
        left join deduped_claims
            on deduped_records.data_source = deduped_claims.data_source
        left join deduped_patients
            on deduped_records.data_source = deduped_patients.data_source

)

select * from final_cte
