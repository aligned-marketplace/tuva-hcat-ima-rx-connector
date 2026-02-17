with adjusted_claims as (

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
        , sum_paid_amount
        , sum_allowed_amount
        , sum_coinsurance_amount
        , sum_copayment_amount
        , sum_deductible_amount
    from {{ ref('int_medical_claim_adr') }}
    where row_num = 1

)

, seed_provider as (

    select
          npi
        , entity_type_code
    from {{ ref('terminology__provider') }}

)

, add_header_claim_dates as (


    select
          claim_id
        , min(claim_line_start_date) as min_claim_start_date
        , max(claim_line_end_date) as max_claim_end_date
    from adjusted_claims
    group by claim_id

)

/*
After the application of ADR logic, there are still some claims
with negative financials. This happens for roughly 0.1% of records
and we hold them from the final output.
*/
, claims_to_exclude as (
  select
    distinct claim_id
  from adjusted_claims
  where (
    sum_paid_amount < 0
    or sum_allowed_amount < 0
    or sum_coinsurance_amount < 0
    or sum_copayment_amount < 0
    or sum_deductible_amount < 0
  )
)

, add_fields as (

    select
          adjusted_claims.claim_id
        , adjusted_claims.claim_line_number
        , case
             when adjusted_claims.bill_type_code is not null
               then 'institutional'
             when adjusted_claims.place_of_service_code is not null
               then 'professional'
             else 'undetermined'
          end as claim_type
        , adjusted_claims.member_id as person_id
        , adjusted_claims.member_id
        , adjusted_claims.payer
        , adjusted_claims.plan
        , add_header_claim_dates.min_claim_start_date as claim_start_date
        , add_header_claim_dates.max_claim_end_date as claim_end_date
        , adjusted_claims.claim_line_start_date
        , adjusted_claims.claim_line_end_date
        , null as admission_date
        , null as discharge_date
        , null as admit_source_code
        , null as admit_type_code
        , adjusted_claims.discharge_disposition_code
        , adjusted_claims.place_of_service_code
        , adjusted_claims.bill_type_code
        , adjusted_claims.drg_code_type
        , adjusted_claims.drg_code
        , adjusted_claims.revenue_center_code
        , adjusted_claims.service_unit_quantity
        , adjusted_claims.hcpcs_code
        , adjusted_claims.hcpcs_modifier_1
        , null as hcpcs_modifier_2
        , null as hcpcs_modifier_3
        , null as hcpcs_modifier_4
        , null as hcpcs_modifier_5
        , case
            when seed_provider.entity_type_code = 1
            then adjusted_claims.billing_npi
            else null
          end as rendering_npi
        , null as rendering_tin
        , adjusted_claims.billing_npi
        , adjusted_claims.billing_tin
        , case
            when seed_provider.entity_type_code = 2
            then adjusted_claims.billing_npi
            else null
          end as facility_npi
        , adjusted_claims.paid_date
        , adjusted_claims.sum_paid_amount as paid_amount
        , adjusted_claims.sum_allowed_amount as allowed_amount
        , null as charge_amount
        , adjusted_claims.sum_coinsurance_amount as coinsurance_amount
        , adjusted_claims.sum_copayment_amount as copayment_amount
        , adjusted_claims.sum_deductible_amount as deductible_amount
        , null as total_cost_amount
        , case
            when coalesce(
                  adjusted_claims.diagnosis_code_1
                , adjusted_claims.diagnosis_code_2
                , adjusted_claims.diagnosis_code_3
                , adjusted_claims.diagnosis_code_4
                , adjusted_claims.diagnosis_code_5
                , adjusted_claims.diagnosis_code_6
                , adjusted_claims.diagnosis_code_7
                , adjusted_claims.diagnosis_code_8
                , adjusted_claims.diagnosis_code_9
                , adjusted_claims.diagnosis_code_10
            ) is not null
            then
                case
                    when adjusted_claims.diagnosis_code_type = '10' then 'icd-10-cm'
                    when adjusted_claims.diagnosis_code_type = '9' then 'icd-9-cm'
                    else null
                end
            else null
          end as diagnosis_code_type
        , adjusted_claims.diagnosis_code_1
        , adjusted_claims.diagnosis_code_2
        , adjusted_claims.diagnosis_code_3
        , adjusted_claims.diagnosis_code_4
        , adjusted_claims.diagnosis_code_5
        , adjusted_claims.diagnosis_code_6
        , adjusted_claims.diagnosis_code_7
        , adjusted_claims.diagnosis_code_8
        , adjusted_claims.diagnosis_code_9
        , adjusted_claims.diagnosis_code_10
        , null as diagnosis_code_11
        , null as diagnosis_code_12
        , null as diagnosis_code_13
        , null as diagnosis_code_14
        , null as diagnosis_code_15
        , null as diagnosis_code_16
        , null as diagnosis_code_17
        , null as diagnosis_code_18
        , null as diagnosis_code_19
        , null as diagnosis_code_20
        , null as diagnosis_code_21
        , null as diagnosis_code_22
        , null as diagnosis_code_23
        , null as diagnosis_code_24
        , null as diagnosis_code_25
        , null as diagnosis_poa_1
        , null as diagnosis_poa_2
        , null as diagnosis_poa_3
        , null as diagnosis_poa_4
        , null as diagnosis_poa_5
        , null as diagnosis_poa_6
        , null as diagnosis_poa_7
        , null as diagnosis_poa_8
        , null as diagnosis_poa_9
        , null as diagnosis_poa_10
        , null as diagnosis_poa_11
        , null as diagnosis_poa_12
        , null as diagnosis_poa_13
        , null as diagnosis_poa_14
        , null as diagnosis_poa_15
        , null as diagnosis_poa_16
        , null as diagnosis_poa_17
        , null as diagnosis_poa_18
        , null as diagnosis_poa_19
        , null as diagnosis_poa_20
        , null as diagnosis_poa_21
        , null as diagnosis_poa_22
        , null as diagnosis_poa_23
        , null as diagnosis_poa_24
        , null as diagnosis_poa_25
        , case
            when coalesce(
                  adjusted_claims.procedure_code_1
                , adjusted_claims.procedure_code_2
                , adjusted_claims.procedure_code_3
                , adjusted_claims.procedure_code_4
                , adjusted_claims.procedure_code_5
                , adjusted_claims.procedure_code_6
            ) is not null
            then
                case
                    when adjusted_claims.procedure_code_type = '10' then 'icd-10-pcs'
                    when adjusted_claims.procedure_code_type = '9' then 'icd-9-pcs'
                    else null
                end
            else null
          end as procedure_code_type
        , adjusted_claims.procedure_code_1
        , adjusted_claims.procedure_code_2
        , adjusted_claims.procedure_code_3
        , adjusted_claims.procedure_code_4
        , adjusted_claims.procedure_code_5
        , adjusted_claims.procedure_code_6
        , null as procedure_code_7
        , null as procedure_code_8
        , null as procedure_code_9
        , null as procedure_code_10
        , null as procedure_code_11
        , null as procedure_code_12
        , null as procedure_code_13
        , null as procedure_code_14
        , null as procedure_code_15
        , null as procedure_code_16
        , null as procedure_code_17
        , null as procedure_code_18
        , null as procedure_code_19
        , null as procedure_code_20
        , null as procedure_code_21
        , null as procedure_code_22
        , null as procedure_code_23
        , null as procedure_code_24
        , null as procedure_code_25
        , null as procedure_date_1
        , null as procedure_date_2
        , null as procedure_date_3
        , null as procedure_date_4
        , null as procedure_date_5
        , null as procedure_date_6
        , null as procedure_date_7
        , null as procedure_date_8
        , null as procedure_date_9
        , null as procedure_date_10
        , null as procedure_date_11
        , null as procedure_date_12
        , null as procedure_date_13
        , null as procedure_date_14
        , null as procedure_date_15
        , null as procedure_date_16
        , null as procedure_date_17
        , null as procedure_date_18
        , null as procedure_date_19
        , null as procedure_date_20
        , null as procedure_date_21
        , null as procedure_date_22
        , null as procedure_date_23
        , null as procedure_date_24
        , null as procedure_date_25
        , null as in_network_flag
        , adjusted_claims.data_source
        , adjusted_claims.file_name
        , adjusted_claims.file_date
        , adjusted_claims.ingest_datetime
    from adjusted_claims
        left join add_header_claim_dates
            on adjusted_claims.claim_id = add_header_claim_dates.claim_id
        left join seed_provider
            on adjusted_claims.billing_npi = seed_provider.npi
        left join claims_to_exclude as exclusions
            on adjusted_claims.claim_id = exclusions.claim_id
    where exclusions.claim_id is null

)

, data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(claim_line_number as integer) as claim_line_number
        , cast(claim_type as {{ dbt.type_string() }}) as claim_type
        , cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(claim_start_date as date) as claim_start_date
        , cast(claim_end_date as date) as claim_end_date
        , cast(claim_line_start_date as date) as claim_line_start_date
        , cast(claim_line_end_date as date) as claim_line_end_date
        , cast(admission_date as date) as admission_date
        , cast(discharge_date as date) as discharge_date
        , cast(admit_source_code as {{ dbt.type_string() }}) as admit_source_code
        , cast(admit_type_code as {{ dbt.type_string() }}) as admit_type_code
        , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
        , cast(place_of_service_code as {{ dbt.type_string() }}) as place_of_service_code
        , cast(bill_type_code as {{ dbt.type_string() }}) as bill_type_code
        , cast(drg_code_type as {{ dbt.type_string() }}) as drg_code_type
        , cast(drg_code as {{ dbt.type_string() }}) as drg_code
        , cast(revenue_center_code as {{ dbt.type_string() }}) as revenue_center_code
        , cast(service_unit_quantity as integer) as service_unit_quantity
        , cast(hcpcs_code as {{ dbt.type_string() }}) as hcpcs_code
        , cast(hcpcs_modifier_1 as {{ dbt.type_string() }}) as hcpcs_modifier_1
        , cast(hcpcs_modifier_2 as {{ dbt.type_string() }}) as hcpcs_modifier_2
        , cast(hcpcs_modifier_3 as {{ dbt.type_string() }}) as hcpcs_modifier_3
        , cast(hcpcs_modifier_4 as {{ dbt.type_string() }}) as hcpcs_modifier_4
        , cast(hcpcs_modifier_5 as {{ dbt.type_string() }}) as hcpcs_modifier_5
        , cast(rendering_npi as {{ dbt.type_string() }}) as rendering_npi
        , cast(rendering_tin as {{ dbt.type_string() }}) as rendering_tin
        , cast(billing_npi as {{ dbt.type_string() }}) as billing_npi
        , cast(billing_tin as {{ dbt.type_string() }}) as billing_tin
        , cast(facility_npi as {{ dbt.type_string() }}) as facility_npi
        , cast(paid_date as date) as paid_date
        , cast(paid_amount as numeric(38,2)) as paid_amount
        , cast(allowed_amount as numeric(38,2)) as allowed_amount
        , cast(charge_amount as numeric(38,2)) as charge_amount
        , cast(coinsurance_amount as numeric(38,2)) as coinsurance_amount
        , cast(copayment_amount as numeric(38,2)) as copayment_amount
        , cast(deductible_amount as numeric(38,2)) as deductible_amount
        , cast(total_cost_amount as numeric(38,2)) as total_cost_amount
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
        , cast(diagnosis_code_11 as {{ dbt.type_string() }}) as diagnosis_code_11
        , cast(diagnosis_code_12 as {{ dbt.type_string() }}) as diagnosis_code_12
        , cast(diagnosis_code_13 as {{ dbt.type_string() }}) as diagnosis_code_13
        , cast(diagnosis_code_14 as {{ dbt.type_string() }}) as diagnosis_code_14
        , cast(diagnosis_code_15 as {{ dbt.type_string() }}) as diagnosis_code_15
        , cast(diagnosis_code_16 as {{ dbt.type_string() }}) as diagnosis_code_16
        , cast(diagnosis_code_17 as {{ dbt.type_string() }}) as diagnosis_code_17
        , cast(diagnosis_code_18 as {{ dbt.type_string() }}) as diagnosis_code_18
        , cast(diagnosis_code_19 as {{ dbt.type_string() }}) as diagnosis_code_19
        , cast(diagnosis_code_20 as {{ dbt.type_string() }}) as diagnosis_code_20
        , cast(diagnosis_code_21 as {{ dbt.type_string() }}) as diagnosis_code_21
        , cast(diagnosis_code_22 as {{ dbt.type_string() }}) as diagnosis_code_22
        , cast(diagnosis_code_23 as {{ dbt.type_string() }}) as diagnosis_code_23
        , cast(diagnosis_code_24 as {{ dbt.type_string() }}) as diagnosis_code_24
        , cast(diagnosis_code_25 as {{ dbt.type_string() }}) as diagnosis_code_25
        , cast(diagnosis_poa_1 as {{ dbt.type_string() }}) as diagnosis_poa_1
        , cast(diagnosis_poa_2 as {{ dbt.type_string() }}) as diagnosis_poa_2
        , cast(diagnosis_poa_3 as {{ dbt.type_string() }}) as diagnosis_poa_3
        , cast(diagnosis_poa_4 as {{ dbt.type_string() }}) as diagnosis_poa_4
        , cast(diagnosis_poa_5 as {{ dbt.type_string() }}) as diagnosis_poa_5
        , cast(diagnosis_poa_6 as {{ dbt.type_string() }}) as diagnosis_poa_6
        , cast(diagnosis_poa_7 as {{ dbt.type_string() }}) as diagnosis_poa_7
        , cast(diagnosis_poa_8 as {{ dbt.type_string() }}) as diagnosis_poa_8
        , cast(diagnosis_poa_9 as {{ dbt.type_string() }}) as diagnosis_poa_9
        , cast(diagnosis_poa_10 as {{ dbt.type_string() }}) as diagnosis_poa_10
        , cast(diagnosis_poa_11 as {{ dbt.type_string() }}) as diagnosis_poa_11
        , cast(diagnosis_poa_12 as {{ dbt.type_string() }}) as diagnosis_poa_12
        , cast(diagnosis_poa_13 as {{ dbt.type_string() }}) as diagnosis_poa_13
        , cast(diagnosis_poa_14 as {{ dbt.type_string() }}) as diagnosis_poa_14
        , cast(diagnosis_poa_15 as {{ dbt.type_string() }}) as diagnosis_poa_15
        , cast(diagnosis_poa_16 as {{ dbt.type_string() }}) as diagnosis_poa_16
        , cast(diagnosis_poa_17 as {{ dbt.type_string() }}) as diagnosis_poa_17
        , cast(diagnosis_poa_18 as {{ dbt.type_string() }}) as diagnosis_poa_18
        , cast(diagnosis_poa_19 as {{ dbt.type_string() }}) as diagnosis_poa_19
        , cast(diagnosis_poa_20 as {{ dbt.type_string() }}) as diagnosis_poa_20
        , cast(diagnosis_poa_21 as {{ dbt.type_string() }}) as diagnosis_poa_21
        , cast(diagnosis_poa_22 as {{ dbt.type_string() }}) as diagnosis_poa_22
        , cast(diagnosis_poa_23 as {{ dbt.type_string() }}) as diagnosis_poa_23
        , cast(diagnosis_poa_24 as {{ dbt.type_string() }}) as diagnosis_poa_24
        , cast(diagnosis_poa_25 as {{ dbt.type_string() }}) as diagnosis_poa_25
        , cast(procedure_code_type as {{ dbt.type_string() }}) as procedure_code_type
        , cast(procedure_code_1 as {{ dbt.type_string() }}) as procedure_code_1
        , cast(procedure_code_2 as {{ dbt.type_string() }}) as procedure_code_2
        , cast(procedure_code_3 as {{ dbt.type_string() }}) as procedure_code_3
        , cast(procedure_code_4 as {{ dbt.type_string() }}) as procedure_code_4
        , cast(procedure_code_5 as {{ dbt.type_string() }}) as procedure_code_5
        , cast(procedure_code_6 as {{ dbt.type_string() }}) as procedure_code_6
        , cast(procedure_code_7 as {{ dbt.type_string() }}) as procedure_code_7
        , cast(procedure_code_8 as {{ dbt.type_string() }}) as procedure_code_8
        , cast(procedure_code_9 as {{ dbt.type_string() }}) as procedure_code_9
        , cast(procedure_code_10 as {{ dbt.type_string() }}) as procedure_code_10
        , cast(procedure_code_11 as {{ dbt.type_string() }}) as procedure_code_11
        , cast(procedure_code_12 as {{ dbt.type_string() }}) as procedure_code_12
        , cast(procedure_code_13 as {{ dbt.type_string() }}) as procedure_code_13
        , cast(procedure_code_14 as {{ dbt.type_string() }}) as procedure_code_14
        , cast(procedure_code_15 as {{ dbt.type_string() }}) as procedure_code_15
        , cast(procedure_code_16 as {{ dbt.type_string() }}) as procedure_code_16
        , cast(procedure_code_17 as {{ dbt.type_string() }}) as procedure_code_17
        , cast(procedure_code_18 as {{ dbt.type_string() }}) as procedure_code_18
        , cast(procedure_code_19 as {{ dbt.type_string() }}) as procedure_code_19
        , cast(procedure_code_20 as {{ dbt.type_string() }}) as procedure_code_20
        , cast(procedure_code_21 as {{ dbt.type_string() }}) as procedure_code_21
        , cast(procedure_code_22 as {{ dbt.type_string() }}) as procedure_code_22
        , cast(procedure_code_23 as {{ dbt.type_string() }}) as procedure_code_23
        , cast(procedure_code_24 as {{ dbt.type_string() }}) as procedure_code_24
        , cast(procedure_code_25 as {{ dbt.type_string() }}) as procedure_code_25
        , cast(procedure_date_1 as date) as procedure_date_1
        , cast(procedure_date_2 as date) as procedure_date_2
        , cast(procedure_date_3 as date) as procedure_date_3
        , cast(procedure_date_4 as date) as procedure_date_4
        , cast(procedure_date_5 as date) as procedure_date_5
        , cast(procedure_date_6 as date) as procedure_date_6
        , cast(procedure_date_7 as date) as procedure_date_7
        , cast(procedure_date_8 as date) as procedure_date_8
        , cast(procedure_date_9 as date) as procedure_date_9
        , cast(procedure_date_10 as date) as procedure_date_10
        , cast(procedure_date_11 as date) as procedure_date_11
        , cast(procedure_date_12 as date) as procedure_date_12
        , cast(procedure_date_13 as date) as procedure_date_13
        , cast(procedure_date_14 as date) as procedure_date_14
        , cast(procedure_date_15 as date) as procedure_date_15
        , cast(procedure_date_16 as date) as procedure_date_16
        , cast(procedure_date_17 as date) as procedure_date_17
        , cast(procedure_date_18 as date) as procedure_date_18
        , cast(procedure_date_19 as date) as procedure_date_19
        , cast(procedure_date_20 as date) as procedure_date_20
        , cast(procedure_date_21 as date) as procedure_date_21
        , cast(procedure_date_22 as date) as procedure_date_22
        , cast(procedure_date_23 as date) as procedure_date_23
        , cast(procedure_date_24 as date) as procedure_date_24
        , cast(procedure_date_25 as date) as procedure_date_25
        , cast(in_network_flag as integer) as in_network_flag
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(file_date as date) as file_date
        , cast(ingest_datetime as datetime) as ingest_datetime
    from add_fields

)

select
      claim_id
    , claim_line_number
    , claim_type
    , person_id
    , member_id
    , payer
    , plan
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , place_of_service_code
    , bill_type_code
    , drg_code_type
    , drg_code
    , revenue_center_code
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , rendering_npi
    , rendering_tin
    , billing_npi
    , billing_tin
    , facility_npi
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
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
    , diagnosis_code_11
    , diagnosis_code_12
    , diagnosis_code_13
    , diagnosis_code_14
    , diagnosis_code_15
    , diagnosis_code_16
    , diagnosis_code_17
    , diagnosis_code_18
    , diagnosis_code_19
    , diagnosis_code_20
    , diagnosis_code_21
    , diagnosis_code_22
    , diagnosis_code_23
    , diagnosis_code_24
    , diagnosis_code_25
    , diagnosis_poa_1
    , diagnosis_poa_2
    , diagnosis_poa_3
    , diagnosis_poa_4
    , diagnosis_poa_5
    , diagnosis_poa_6
    , diagnosis_poa_7
    , diagnosis_poa_8
    , diagnosis_poa_9
    , diagnosis_poa_10
    , diagnosis_poa_11
    , diagnosis_poa_12
    , diagnosis_poa_13
    , diagnosis_poa_14
    , diagnosis_poa_15
    , diagnosis_poa_16
    , diagnosis_poa_17
    , diagnosis_poa_18
    , diagnosis_poa_19
    , diagnosis_poa_20
    , diagnosis_poa_21
    , diagnosis_poa_22
    , diagnosis_poa_23
    , diagnosis_poa_24
    , diagnosis_poa_25
    , procedure_code_type
    , procedure_code_1
    , procedure_code_2
    , procedure_code_3
    , procedure_code_4
    , procedure_code_5
    , procedure_code_6
    , procedure_code_7
    , procedure_code_8
    , procedure_code_9
    , procedure_code_10
    , procedure_code_11
    , procedure_code_12
    , procedure_code_13
    , procedure_code_14
    , procedure_code_15
    , procedure_code_16
    , procedure_code_17
    , procedure_code_18
    , procedure_code_19
    , procedure_code_20
    , procedure_code_21
    , procedure_code_22
    , procedure_code_23
    , procedure_code_24
    , procedure_code_25
    , procedure_date_1
    , procedure_date_2
    , procedure_date_3
    , procedure_date_4
    , procedure_date_5
    , procedure_date_6
    , procedure_date_7
    , procedure_date_8
    , procedure_date_9
    , procedure_date_10
    , procedure_date_11
    , procedure_date_12
    , procedure_date_13
    , procedure_date_14
    , procedure_date_15
    , procedure_date_16
    , procedure_date_17
    , procedure_date_18
    , procedure_date_19
    , procedure_date_20
    , procedure_date_21
    , procedure_date_22
    , procedure_date_23
    , procedure_date_24
    , procedure_date_25
    , in_network_flag
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from data_types
