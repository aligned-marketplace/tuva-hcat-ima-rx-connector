with source_data as (

    select
        _file
        , _line
        , _modified
        , _fivetran_synced
        , provider_type
        , employee_id
        , paid_amount
        , claim_paid_date
        , group_number
        , provider_zip
        , member_last_name
        , place_of_service
        , type_of_service
        , revenue_code
        , claim_line_inclu_date
        , member_first_name
        , discharge_status
        , modifier
        , cpt_procedure_code
        , icd_1
        , rx_ndc
        , group_division_name
        , icd_4
        , member_claimant_relationship
        , icd_5
        , icd_2
        , icd_3
        , icd_8
        , icd_9
        , icd_6
        , icd_7
        , claim_line_incur_date
        , provider_type_description
        , bill_type
        , claim_line_allow_amount
        , provider_city
        , member_claimant_dob
        , primary_icd
        , billing_provider_state
        , member_ssn
        , claim_line_deductable_amount
        , billing_provider_id
        , network_affiliation
        , provider_id
        , provider_npi
        , billing_provider_zip
        , billing_provider_city
        , other_insurance_amount
        , claim_line_copay_amount
        , provider_name
        , member_claimant_gender
        , provider_address
        , claim_status
        , claim_line_coinsurance_amount
        , icdp_2
        , subscriber_ssn
        , icdp_3
        , icdp_4
        , icdp_5
        , claim_received_date
        , claim_number
        , icdp_1
        , billing_provider_address
        , claim_line_sequence_no
        , provider_state
        , claim_line_units
        , icdp_6
        , group_division
        , member_id
        , icd_flag
        , billing_provider_name
    from {{ source('select_health','medical_claim') }}
)

, mapping as (

    select
          group_number
        , group_division
        , group_division_name
        , employee_id
        , member_id
        , member_claimant_relationship
        , member_claimant_gender
        , member_claimant_dob
        , claim_received_date
        , claim_paid_date as paid_date
        , claim_number as claim_id
        , claim_line_sequence_no as claim_line_number
        , place_of_service as place_of_service_code
        , type_of_service
        , bill_type as bill_type_code
        , revenue_code as revenue_center_code
        , primary_icd as diagnosis_code_1
        , icd_1 as diagnosis_code_2
        , icd_2 as diagnosis_code_3
        , icd_3 as diagnosis_code_4
        , icd_4 as diagnosis_code_5
        , icd_5 as diagnosis_code_6
        , icd_6 as diagnosis_code_7
        , icd_7 as diagnosis_code_8
        , icd_8 as diagnosis_code_9
        , icd_9 as diagnosis_code_10
        , null as ms_drg_code
        , null as drg_identifier
        , discharge_status as discharge_disposition_code
        , cpt_procedure_code as hcpcs_code
        , modifier as hcpcs_modifier_1
        , icdp_1 as procedure_code_1
        , icdp_2 as procedure_code_2
        , icdp_3 as procedure_code_3
        , icdp_4 as procedure_code_4
        , icdp_5 as procedure_code_5
        , icdp_6 as procedure_code_6
        , provider_id
        , provider_name
        , provider_address
        , provider_city
        , provider_state
        , provider_zip
        , network_affiliation
        , provider_type
        , provider_type_description
        , claim_line_incur_date as claim_line_start_date
        , claim_line_inclu_date as claim_line_end_date
        , other_insurance_amount
        , claim_line_allow_amount as allowed_amount
        , claim_line_copay_amount as copayment_amount
        , claim_line_coinsurance_amount as coinsurance_amount
        , claim_line_deductable_amount as deductible_amount
        , paid_amount as paid_amount
        , claim_line_units as service_unit_quantity
        , provider_npi as billing_npi
        , member_first_name
        , member_last_name
        , member_ssn
        , subscriber_ssn
        , icd_flag as diagnosis_code_type
        , icd_flag as procedure_code_type
        , rx_ndc
        , claim_status
        , billing_provider_id as billing_tin
        , billing_provider_name
        , billing_provider_address
        , billing_provider_city
        , billing_provider_state
        , billing_provider_zip
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
        , cast(employee_id as {{ dbt.type_string() }}) as employee_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(member_claimant_relationship as {{ dbt.type_string() }}) as member_claimant_relationship
        , cast(member_claimant_gender as {{ dbt.type_string() }}) as member_claimant_gender
        , cast(member_claimant_dob as {{ dbt.type_string() }}) as member_claimant_dob
        , cast(claim_received_date as {{ dbt.type_string() }}) as claim_received_date
        , cast(paid_date as {{ dbt.type_string() }}) as paid_date
        , cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(claim_line_number as {{ dbt.type_string() }}) as claim_line_number
        , cast(place_of_service_code as {{ dbt.type_string() }}) as place_of_service_code
        , cast(type_of_service as {{ dbt.type_string() }}) as type_of_service
        , cast(bill_type_code as {{ dbt.type_string() }}) as bill_type_code
        , cast(revenue_center_code as {{ dbt.type_string() }}) as revenue_center_code
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
        , cast(ms_drg_code as {{ dbt.type_string() }}) as ms_drg_code
        , cast(drg_identifier as {{ dbt.type_string() }}) as drg_identifier
        , cast(discharge_disposition_code as {{ dbt.type_string() }}) as discharge_disposition_code
        , cast(hcpcs_code as {{ dbt.type_string() }}) as hcpcs_code
        , cast(hcpcs_modifier_1 as {{ dbt.type_string() }}) as hcpcs_modifier_1
        , cast(procedure_code_1 as {{ dbt.type_string() }}) as procedure_code_1
        , cast(procedure_code_2 as {{ dbt.type_string() }}) as procedure_code_2
        , cast(procedure_code_3 as {{ dbt.type_string() }}) as procedure_code_3
        , cast(procedure_code_4 as {{ dbt.type_string() }}) as procedure_code_4
        , cast(procedure_code_5 as {{ dbt.type_string() }}) as procedure_code_5
        , cast(procedure_code_6 as {{ dbt.type_string() }}) as procedure_code_6
        , cast(provider_id as {{ dbt.type_string() }}) as provider_id
        , cast(provider_name as {{ dbt.type_string() }}) as provider_name
        , cast(provider_address as {{ dbt.type_string() }}) as provider_address
        , cast(provider_city as {{ dbt.type_string() }}) as provider_city
        , cast(provider_state as {{ dbt.type_string() }}) as provider_state
        , cast(provider_zip as {{ dbt.type_string() }}) as provider_zip
        , cast(network_affiliation as {{ dbt.type_string() }}) as network_affiliation
        , cast(provider_type as {{ dbt.type_string() }}) as provider_type
        , cast(provider_type_description  as {{ dbt.type_string() }}) as provider_type_description
        , cast(claim_line_start_date as {{ dbt.type_string() }}) as claim_line_start_date
        , cast(claim_line_end_date as {{ dbt.type_string() }}) as claim_line_end_date
        , cast(other_insurance_amount as {{ dbt.type_string() }}) as other_insurance_amount
        , cast(allowed_amount as {{ dbt.type_string() }}) as allowed_amount
        , cast(copayment_amount as {{ dbt.type_string() }}) as copayment_amount
        , cast(coinsurance_amount as {{ dbt.type_string() }}) as coinsurance_amount
        , cast(deductible_amount as {{ dbt.type_string() }}) as deductible_amount
        , cast(paid_amount as {{ dbt.type_string() }}) as paid_amount
        , cast(service_unit_quantity as {{ dbt.type_string() }}) as service_unit_quantity
        , cast(billing_npi as {{ dbt.type_string() }}) as billing_npi
        , cast(member_first_name as {{ dbt.type_string() }}) as member_first_name
        , cast(member_last_name as {{ dbt.type_string() }}) as member_last_name
        , cast(member_ssn as {{ dbt.type_string() }}) as member_ssn
        , cast(subscriber_ssn as {{ dbt.type_string() }}) as subscriber_ssn
        , cast(diagnosis_code_type as {{ dbt.type_string() }}) as diagnosis_code_type
        , cast(procedure_code_type as {{ dbt.type_string() }}) as procedure_code_type
        , cast(rx_ndc as {{ dbt.type_string() }}) as rx_ndc
        , cast(claim_status as {{ dbt.type_string() }}) as claim_status
        , cast(billing_tin as {{ dbt.type_string() }}) as billing_tin
        , cast(billing_provider_name as {{ dbt.type_string() }}) as billing_provider_name
        , cast(billing_provider_address as {{ dbt.type_string() }}) as billing_provider_address
        , cast(billing_provider_city as {{ dbt.type_string() }}) as billing_provider_city
        , cast(billing_provider_state as {{ dbt.type_string() }}) as billing_provider_state
        , cast(billing_provider_zip as {{ dbt.type_string() }}) as billing_provider_zip
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
    , file_name
    , file_date
    , ingest_datetime
from data_types

union all

select
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
    , file_name
    , file_date
    , ingest_datetime
from {{ source('tuva_historical_select_health','medical_claim') }}
