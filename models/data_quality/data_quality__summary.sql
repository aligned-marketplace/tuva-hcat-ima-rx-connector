with eligibility as (

    select
          source_type
        , step
        , description
        , data_source
        , records
        , null as claims
        , patients
    from {{ ref('data_quality__eligibility_dedupe_accounting') }}

)

, medical_claim as (

    select
          source_type
        , step
        , description
        , data_source
        , records
        , claims
        , patients
    from {{ ref('data_quality__medical_claim_dedupe_accounting') }}

)

, pharmacy_claim as (

    select
          source_type
        , step
        , description
        , data_source
        , records
        , claims
        , patients
    from {{ ref('data_quality__pharmacy_claim_dedupe_accounting') }}

)

, unioned as (

    select * from eligibility
    union all
    select * from medical_claim
    union all
    select * from pharmacy_claim

)

select * from unioned