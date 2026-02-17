with med_claim as (

    select
          data_source
        , claim_id
        , substr(cast(claim_start_date as varchar),1,7) as year_month
        , paid_amount
        , allowed_amount
        , claim_type
    from {{ ref('medical_claim') }}

)

, pharm_claim as (

    select
          data_source
        , claim_id
        , substr(cast(coalesce(dispensing_date,paid_date) as varchar),1,7) as year_month
        , paid_amount
        , allowed_amount
    from {{ ref('pharmacy_claim') }}

),

medical as (

    select
          data_source
        , year_month
        , count(distinct claim_id) as claims
        , sum(paid_amount) as paid_amount
        , sum(allowed_amount) as allowed_amount
    from med_claim
    group by
          data_source
        , year_month

),

institutional as (

    select
          data_source
        , year_month
        , count(distinct claim_id) as claims
        , sum(paid_amount) as paid_amount
        , sum(allowed_amount) as allowed_amount
    from med_claim
    where claim_type = 'institutional'
    group by
          data_source
        , year_month

)

, professional as (

    select
          data_source
        , year_month
        , count(distinct claim_id) as claims
        , sum(paid_amount) as paid_amount
        , sum(allowed_amount) as allowed_amount
    from med_claim
    where claim_type = 'professional'
    group by
          data_source
        , year_month

)

, pharmacy as (

    select
          data_source
        , year_month
        , count(distinct claim_id) as claims
        , sum(paid_amount) as paid_amount
        , sum(allowed_amount) as allowed_amount
    from pharm_claim
    group by
          data_source
        , year_month

)

, final_cte as (

    select
          medical.data_source
        , medical.year_month as year_month
        , medical.claims as medical_claims
        , medical.paid_amount as medical_paid_amount
        , medical.allowed_amount as medical_allowed_amount
        , institutional.claims as institutional_claims
        , institutional.paid_amount as institutional_paid_amount
        , institutional.allowed_amount as institutional_allowed_amount
        , professional.claims as professional_claims
        , professional.paid_amount as professional_paid_amount
        , professional.allowed_amount as professional_allowed_amount
        , pharmacy.claims as pharmacy_claims
        , pharmacy.paid_amount as pharmacy_paid_amount
        , pharmacy.allowed_amount as pharmacy_allowed_amount
    from medical
        left join institutional
            on medical.year_month = institutional.year_month
            and medical.data_source = institutional.data_source
        left join professional
            on medical.year_month = professional.year_month
            and medical.data_source = professional.data_source
        left join pharmacy
            on medical.year_month = pharmacy.year_month
            and medical.data_source = pharmacy.data_source

)

select * from final_cte