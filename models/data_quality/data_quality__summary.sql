with pharmacy_claim as (

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

select * from pharmacy_claim
