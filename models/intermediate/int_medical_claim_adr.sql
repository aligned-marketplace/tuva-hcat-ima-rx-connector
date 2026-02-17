with staged_data as (

    select *, row_number() over (
        partition by
              group_number
            , group_division
            , group_division_name
            , employee_id
            , member_id
            , member_claimant_relationship
            , member_claimant_gender
            , member_claimant_dob
            , claim_received_date
            , paid_date
            , claim_id
            , claim_line_number
            , place_of_service_code
            , type_of_service
            , bill_type_code
            , revenue_center_code
            , diagnosis_code_1
            , diagnosis_code_2
            , diagnosis_code_3
            , diagnosis_code_4
            , diagnosis_code_5
            , diagnosis_code_6
            , diagnosis_code_7
            , diagnosis_code_8
            , diagnosis_code_9
            , diagnosis_code_10
            , ms_drg_code
            , drg_identifier
            , discharge_disposition_code
            , hcpcs_code
            , hcpcs_modifier_1
            , procedure_code_1
            , procedure_code_2
            , procedure_code_3
            , procedure_code_4
            , procedure_code_5
            , procedure_code_6
            , provider_id
            , provider_name
            , provider_address
            , provider_city
            , provider_state
            , provider_zip
            , network_affiliation
            , provider_type
            , provider_type_description
            , claim_line_start_date
            , claim_line_end_date
            , other_insurance_amount
            , allowed_amount
            , copayment_amount
            , coinsurance_amount
            , deductible_amount
            , paid_amount
            , service_unit_quantity
            , billing_npi
            , member_first_name
            , member_last_name
            , member_ssn
            , subscriber_ssn
            , diagnosis_code_type
            , procedure_code_type
            , rx_ndc
            , claim_status
            , billing_tin
            , billing_provider_name
            , billing_provider_address
            , billing_provider_city
            , billing_provider_state
            , billing_provider_zip
            , data_source
        order by ingest_datetime desc
      ) as row_num
    from {{ ref('stg_medical_claim') }}
)

, get_first_ingest_datetime as (
    select
        claim_id
        , claim_line_number
        , min(ingest_datetime) as first_ingest_datetime
    from staged_data
    group by claim_id, claim_line_number
)

/* source fields not mapped are commented out */
, staged_data_deduped as (

    select
        /*group_number*/
        /*, group_division*/
        /*, group_division_name*/
        /*, employee_id*/
          member_id
        /*, member_claimant_relationship*/
        /*, member_claimant_gender*/
        /*, member_claimant_dob*/
        /*, claim_received_date*/
        , paid_date
        , sd.claim_id
        , sd.claim_line_number
        , place_of_service_code
        /*, type_of_service*/
        , bill_type_code
        , revenue_center_code
        , diagnosis_code_1
        , diagnosis_code_2
        , diagnosis_code_3
        , diagnosis_code_4
        , diagnosis_code_5
        , diagnosis_code_6
        , diagnosis_code_7
        , diagnosis_code_8
        , diagnosis_code_9
        , diagnosis_code_10
        , ms_drg_code
        /*, drg_identifier*/
        , discharge_disposition_code
        , hcpcs_code
        , hcpcs_modifier_1
        , procedure_code_1
        , procedure_code_2
        , procedure_code_3
        , procedure_code_4
        , procedure_code_5
        , procedure_code_6
        /*, provider_id*/
        /*, provider_name*/
        /*, provider_address*/
        /*, provider_city*/
        /*, provider_state*/
        /*, provider_zip*/
        /*, network_affiliation*/
        /*, provider_type*/
        /*, provider_type_description*/
        , claim_line_start_date
        , claim_line_end_date
        /*, other_insurance_amount*/
        , allowed_amount
        , copayment_amount
        , coinsurance_amount
        , deductible_amount
        , paid_amount
        , service_unit_quantity
        , billing_npi
        /*, member_first_name*/
        /*, member_last_name*/
        /*, member_ssn*/
        /*, subscriber_ssn*/
        , diagnosis_code_type
        , procedure_code_type
        /*, rx_ndc*/
        , claim_status
        , billing_tin
        /*, billing_provider_name*/
        /*, billing_provider_address*/
        /*, billing_provider_city*/
        /*, billing_provider_state*/
        /*, billing_provider_zip*/
        , data_source
        , file_name
        , file_date
        , ingest_datetime
        , first_ingest_datetime
    from staged_data as sd
    left join get_first_ingest_datetime as gfid
    on sd.claim_id = gfid.claim_id
    and sd.claim_line_number = gfid.claim_line_number
    where row_num = 1

)

, distinct_eligibility as (

    select distinct
          member_id
        , enrollment_start_date
        , enrollment_end_date
        , payer
        , plan
    from {{ ref('eligibility') }}

)

, mapping as (

    select
          coalesce(distinct_eligibility.payer,'Health Catalyst') as payer
        , coalesce(distinct_eligibility.plan,'Select Health') as plan
        , staged_data_deduped.claim_id
        , staged_data_deduped.claim_line_number
        , staged_data_deduped.member_id
        , date(staged_data_deduped.claim_line_start_date) as claim_line_start_date
        , date(staged_data_deduped.claim_line_end_date) as claim_line_end_date
        , staged_data_deduped.discharge_disposition_code
        , staged_data_deduped.place_of_service_code
        , staged_data_deduped.bill_type_code
        , CASE WHEN staged_data_deduped.ms_drg_code IS NOT NULL THEN 'ms-drg' END AS drg_code_type
        , staged_data_deduped.ms_drg_code AS drg_code
        , staged_data_deduped.revenue_center_code
        , staged_data_deduped.service_unit_quantity
        , staged_data_deduped.hcpcs_code
        , staged_data_deduped.hcpcs_modifier_1
        , staged_data_deduped.billing_npi
        , staged_data_deduped.billing_tin
        , date(staged_data_deduped.paid_date) as paid_date
        , staged_data_deduped.paid_amount
        , staged_data_deduped.allowed_amount
        , staged_data_deduped.coinsurance_amount
        , staged_data_deduped.copayment_amount
        , staged_data_deduped.deductible_amount
        , staged_data_deduped.diagnosis_code_type
        , staged_data_deduped.diagnosis_code_1
        , staged_data_deduped.diagnosis_code_2
        , staged_data_deduped.diagnosis_code_3
        , staged_data_deduped.diagnosis_code_4
        , staged_data_deduped.diagnosis_code_5
        , staged_data_deduped.diagnosis_code_6
        , staged_data_deduped.diagnosis_code_7
        , staged_data_deduped.diagnosis_code_8
        , staged_data_deduped.diagnosis_code_9
        , staged_data_deduped.diagnosis_code_10
        , staged_data_deduped.procedure_code_type
        , staged_data_deduped.procedure_code_1
        , staged_data_deduped.procedure_code_2
        , staged_data_deduped.procedure_code_3
        , staged_data_deduped.procedure_code_4
        , staged_data_deduped.procedure_code_5
        , staged_data_deduped.procedure_code_6
        , staged_data_deduped.data_source
        , staged_data_deduped.file_name
        , staged_data_deduped.file_date
        , staged_data_deduped.first_ingest_datetime as ingest_datetime
        , staged_data_deduped.claim_status
    from staged_data_deduped
        left join distinct_eligibility
            on staged_data_deduped.member_id = distinct_eligibility.member_id
            and staged_data_deduped.paid_date
                between distinct_eligibility.enrollment_start_date
                and distinct_eligibility.enrollment_end_date

)

/*
    Here we apply adjustment logic by grouping sets of claim lines by
    claim id and line number then sum the total paid amount.
*/
, claim_line_totals as (

    select
          claim_id
        , abs(claim_line_number) as original_claim_line_number
        , sum(paid_amount) as sum_paid_amount
        , sum(allowed_amount) as sum_allowed_amount
        , sum(coinsurance_amount) as sum_coinsurance_amount
        , sum(copayment_amount) as sum_copayment_amount
        , sum(deductible_amount) as sum_deductible_amount
    from mapping
    group by
          claim_id
        , original_claim_line_number

)

/*
    Next, we use row number to order the claim line sets to get the latest
    version of the claim line.
*/
, claim_line_ordered as (

    select
          mapping.claim_id
        , mapping.claim_line_number
        , mapping.member_id
        , mapping.payer
        , mapping.plan
        , mapping.claim_line_start_date
        , mapping.claim_line_end_date
        , mapping.discharge_disposition_code
        , mapping.place_of_service_code
        , mapping.bill_type_code
        , mapping.drg_code_type
        , mapping.drg_code
        , mapping.revenue_center_code
        , mapping.service_unit_quantity
        , mapping.hcpcs_code
        , mapping.hcpcs_modifier_1
        , mapping.billing_npi
        , mapping.billing_tin
        , mapping.paid_date
        , mapping.paid_amount
        , mapping.allowed_amount
        , mapping.coinsurance_amount
        , mapping.copayment_amount
        , mapping.deductible_amount
        , claim_line_totals.sum_paid_amount
        , claim_line_totals.sum_allowed_amount
        , claim_line_totals.sum_coinsurance_amount
        , claim_line_totals.sum_copayment_amount
        , claim_line_totals.sum_deductible_amount
        , mapping.diagnosis_code_type
        , mapping.diagnosis_code_1
        , mapping.diagnosis_code_2
        , mapping.diagnosis_code_3
        , mapping.diagnosis_code_4
        , mapping.diagnosis_code_5
        , mapping.diagnosis_code_6
        , mapping.diagnosis_code_7
        , mapping.diagnosis_code_8
        , mapping.diagnosis_code_9
        , mapping.diagnosis_code_10
        , mapping.procedure_code_type
        , mapping.procedure_code_1
        , mapping.procedure_code_2
        , mapping.procedure_code_3
        , mapping.procedure_code_4
        , mapping.procedure_code_5
        , mapping.procedure_code_6
        , mapping.data_source
        , mapping.file_name
        , mapping.file_date
        , mapping.ingest_datetime
        , mapping.claim_status
        , row_number() over (
            partition by
                  mapping.claim_id
                , abs(mapping.claim_line_number)
            order by
                  mapping.paid_date desc
                , mapping.claim_line_number desc
          ) as row_num
    from mapping
        left join claim_line_totals
            on mapping.claim_id = claim_line_totals.claim_id
            and abs(mapping.claim_line_number) = claim_line_totals.original_claim_line_number

)

, data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(claim_line_number as integer) as claim_line_number
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(claim_line_start_date as date) as claim_line_start_date
        , cast(claim_line_end_date as date) as claim_line_end_date
        , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
        , cast(place_of_service_code as {{ dbt.type_string() }}) as place_of_service_code
        , cast(bill_type_code as {{ dbt.type_string() }}) as bill_type_code
        , cast(drg_code_type as {{ dbt.type_string() }}) as drg_code_type
        , cast(drg_code as {{ dbt.type_string() }}) as drg_code
        , cast(revenue_center_code as {{ dbt.type_string() }}) as revenue_center_code
        , cast(service_unit_quantity as integer) as service_unit_quantity
        , cast(hcpcs_code as {{ dbt.type_string() }}) as hcpcs_code
        , cast(hcpcs_modifier_1 as {{ dbt.type_string() }}) as hcpcs_modifier_1
        , cast(billing_npi as {{ dbt.type_string() }}) as billing_npi
        , cast(billing_tin as {{ dbt.type_string() }}) as billing_tin
        , cast(paid_date as date) as paid_date
        , cast(paid_amount as numeric(38,2)) as paid_amount
        , cast(allowed_amount as numeric(38,2)) as allowed_amount
        , cast(coinsurance_amount as numeric(38,2)) as coinsurance_amount
        , cast(copayment_amount as numeric(38,2)) as copayment_amount
        , cast(deductible_amount as numeric(38,2)) as deductible_amount
        , cast(sum_paid_amount as numeric(38,2)) as sum_paid_amount
        , cast(sum_allowed_amount as numeric(38,2)) as sum_allowed_amount
        , cast(sum_coinsurance_amount as numeric(38,2)) as sum_coinsurance_amount
        , cast(sum_copayment_amount as numeric(38,2)) as sum_copayment_amount
        , cast(sum_deductible_amount as numeric(38,2)) as sum_deductible_amount
        , cast(diagnosis_code_type as {{ dbt.type_string() }}) as diagnosis_code_type
        , cast(diagnosis_code_1 as {{ dbt.type_string() }}) as diagnosis_code_1
        , cast(diagnosis_code_2 as {{ dbt.type_string() }}) as diagnosis_code_2
        , cast(diagnosis_code_3 as {{ dbt.type_string() }}) as diagnosis_code_3
        , cast(diagnosis_code_4 as {{ dbt.type_string() }}) as diagnosis_code_4
        , cast(diagnosis_code_5 as {{ dbt.type_string() }}) as diagnosis_code_5
        , cast(diagnosis_code_6 as {{ dbt.type_string() }}) as diagnosis_code_6
        , cast(diagnosis_code_7 as {{ dbt.type_string() }}) as diagnosis_code_7
        , cast(diagnosis_code_8 as {{ dbt.type_string() }}) as diagnosis_code_8
        , cast(diagnosis_code_9 as {{ dbt.type_string() }}) as diagnosis_code_9
        , cast(diagnosis_code_10 as {{ dbt.type_string() }}) as diagnosis_code_10
        , cast(procedure_code_type as {{ dbt.type_string() }}) as procedure_code_type
        , cast(procedure_code_1 as {{ dbt.type_string() }}) as procedure_code_1
        , cast(procedure_code_2 as {{ dbt.type_string() }}) as procedure_code_2
        , cast(procedure_code_3 as {{ dbt.type_string() }}) as procedure_code_3
        , cast(procedure_code_4 as {{ dbt.type_string() }}) as procedure_code_4
        , cast(procedure_code_5 as {{ dbt.type_string() }}) as procedure_code_5
        , cast(procedure_code_6 as {{ dbt.type_string() }}) as procedure_code_6
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(file_date as date) as file_date
        , cast(ingest_datetime as datetime) as ingest_datetime
        , cast(claim_status as {{ dbt.type_string() }}) as claim_status
        , cast(row_num as integer) as row_num
    from claim_line_ordered

)

select
      claim_id
    , claim_line_number
    , member_id
    , payer
    , plan
    , claim_line_start_date
    , claim_line_end_date
    , discharge_disposition_code
    , place_of_service_code
    , bill_type_code
    , drg_code_type
    , drg_code
    , revenue_center_code
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , billing_npi
    , billing_tin
    , paid_date
    , paid_amount
    , allowed_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , sum_paid_amount
    , sum_allowed_amount
    , sum_coinsurance_amount
    , sum_copayment_amount
    , sum_deductible_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , diagnosis_code_2
    , diagnosis_code_3
    , diagnosis_code_4
    , diagnosis_code_5
    , diagnosis_code_6
    , diagnosis_code_7
    , diagnosis_code_8
    , diagnosis_code_9
    , diagnosis_code_10
    , procedure_code_type
    , procedure_code_1
    , procedure_code_2
    , procedure_code_3
    , procedure_code_4
    , procedure_code_5
    , procedure_code_6
    , data_source
    , file_name
    , file_date
    , ingest_datetime
    , claim_status
    , row_num
from data_types
