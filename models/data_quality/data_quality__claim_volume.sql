with pharm_claim as (

    select
          data_source
        , claim_id
        , substr(cast(coalesce(dispensing_date,paid_date) as varchar),1,7) as year_month
        , paid_amount
        , allowed_amount
    from {{ ref('pharmacy_claim') }}

)

, pharmacy as (

    select
          data_source
        , year_month
        , count(distinct claim_id) as pharmacy_claims
        , sum(paid_amount) as pharmacy_paid_amount
        , sum(allowed_amount) as pharmacy_allowed_amount
    from pharm_claim
    group by
          data_source
        , year_month

)

select * from pharmacy
