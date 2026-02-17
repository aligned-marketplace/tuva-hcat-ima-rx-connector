with source_data as (

    select
        _file
        , _line
        , _modified
        , _fivetran_synced
        , days_supply
        , retail_mail_order
        , claim_id
        , member_paid
        , employee_id
        , new_or_refill
        , compound_code
        , member_ssn
        , date_filled
        , tier
        , pharmacy_npi
        , plan_paid
        , member_zip_code
        , sales_tax
        , group_number
        , pharmacy_zip
        , pharmacy_nabp
        , drug_name
        , ndc
        , date_prescribed
        , dispensing_fee
        , member_last_name
        , pharmacy_name
        , claimant_relationship
        , date_processed_paid_date
        , member_first_name
        , daw_indicator
        , claimant_dob
        , formulary_non_formulary
        , group_division_name
        , subscriber_ssn
        , prescribing_physician_tin
        , ingredient_cost
        , drug_type
        , claimant_gender
        , quantity_dispensed
        , prescribing_physician_zip
        , group_division
        , prescribing_physician_name
        , member_id
        , prescribing_physician_id
    from {{ source('select_health','pharmacy_claim') }}
)

, mapping as (

    select
          group_number
        , group_division
        , group_division_name
        , employee_id
        , member_id
        , member_zip_code
        , claimant_relationship
        , claimant_gender
        , claimant_dob
        , prescribing_physician_id as prescribing_provider_npi
        , prescribing_physician_name
        , prescribing_physician_zip
        , prescribing_physician_tin
        , pharmacy_nabp
        , pharmacy_name
        , pharmacy_zip
        , claim_id
        , date_prescribed
        , date_filled as dispensing_date
        , date_processed_paid_date as paid_date
        , drug_name
        , ndc as ndc_code
        , formulary_non_formulary
        , tier
        , compound_code
        , quantity_dispensed as quantity
        , days_supply as days_supply
        , drug_type
        , retail_mail_order
        , ingredient_cost
        , dispensing_fee
        , sales_tax
        , member_paid as copayment_amount
        , plan_paid as paid_amount
        , member_first_name
        , member_last_name
        , member_ssn
        , subscriber_ssn
        , new_or_refill as refills
        , daw_indicator
        , null as rx_flag
        , pharmacy_npi as dispensing_provider_npi
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
        , cast(member_zip_code as {{ dbt.type_string() }}) as member_zip_code
        , cast(claimant_relationship as {{ dbt.type_string() }}) as claimant_relationship
        , cast(claimant_gender as {{ dbt.type_string() }}) as claimant_gender
        , cast(claimant_dob as {{ dbt.type_string() }}) as claimant_dob
        , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
        , cast(prescribing_physician_name as {{ dbt.type_string() }}) as prescribing_physician_name
        , cast(prescribing_physician_zip as {{ dbt.type_string() }}) as prescribing_physician_zip
        , cast(prescribing_physician_tin as {{ dbt.type_string() }}) as prescribing_physician_tin
        , cast(pharmacy_nabp as {{ dbt.type_string() }}) as pharmacy_nabp
        , cast(pharmacy_name as {{ dbt.type_string() }}) as pharmacy_name
        , cast(pharmacy_zip as {{ dbt.type_string() }}) as pharmacy_zip
        , cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(date_prescribed as {{ dbt.type_string() }}) as date_prescribed
        , cast(dispensing_date as {{ dbt.type_string() }}) as dispensing_date
        , cast(paid_date as {{ dbt.type_string() }}) as paid_date
        , cast(drug_name as {{ dbt.type_string() }}) as drug_name
        , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
        , cast(formulary_non_formulary as {{ dbt.type_string() }}) as formulary_non_formulary
        , cast(tier as {{ dbt.type_string() }}) as tier
        , cast(compound_code as {{ dbt.type_string() }}) as compound_code
        , cast(quantity as {{ dbt.type_string() }}) as quantity
        , cast(days_supply as {{ dbt.type_string() }}) as days_supply
        , cast(drug_type as {{ dbt.type_string() }}) as drug_type
        , cast(retail_mail_order as {{ dbt.type_string() }}) as retail_mail_order
        , cast(ingredient_cost as {{ dbt.type_string() }}) as ingredient_cost
        , cast(dispensing_fee as {{ dbt.type_string() }}) as dispensing_fee
        , cast(sales_tax as {{ dbt.type_string() }}) as sales_tax
        , cast(copayment_amount as {{ dbt.type_string() }}) as copayment_amount
        , cast(paid_amount as {{ dbt.type_string() }}) as paid_amount
        , cast(member_first_name as {{ dbt.type_string() }}) as member_first_name
        , cast(member_last_name as {{ dbt.type_string() }}) as member_last_name
        , cast(member_ssn as {{ dbt.type_string() }}) as member_ssn
        , cast(subscriber_ssn as {{ dbt.type_string() }}) as subscriber_ssn
        , cast(refills as {{ dbt.type_string() }}) as refills
        , cast(daw_indicator as {{ dbt.type_string() }}) as daw_indicator
        , cast(rx_flag as {{ dbt.type_string() }}) as rx_flag
        , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
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
    , member_zip_code
    , claimant_relationship
    , claimant_gender
    , claimant_dob
    , prescribing_provider_npi
    , prescribing_physician_name
    , prescribing_physician_zip
    , prescribing_physician_tin
    , pharmacy_nabp
    , pharmacy_name
    , pharmacy_zip
    , claim_id
    , date_prescribed
    , dispensing_date
    , paid_date
    , drug_name
    , ndc_code
    , formulary_non_formulary
    , tier
    , compound_code
    , quantity
    , days_supply
    , drug_type
    , retail_mail_order
    , ingredient_cost
    , dispensing_fee
    , sales_tax
    , copayment_amount
    , paid_amount
    , member_first_name
    , member_last_name
    , member_ssn
    , subscriber_ssn
    , refills
    , daw_indicator
    , rx_flag
    , dispensing_provider_npi
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
    , member_zip_code
    , claimant_relationship
    , claimant_gender
    , claimant_dob
    , prescribing_provider_npi
    , prescribing_physician_name
    , prescribing_physician_zip
    , prescribing_physician_tin
    , pharmacy_nabp
    , pharmacy_name
    , pharmacy_zip
    , claim_id
    , date_prescribed
    , dispensing_date
    , paid_date
    , drug_name
    , ndc_code
    , formulary_non_formulary
    , tier
    , compound_code
    , quantity
    , days_supply
    , drug_type
    , retail_mail_order
    , ingredient_cost
    , dispensing_fee
    , sales_tax
    , copayment_amount
    , paid_amount
    , member_first_name
    , member_last_name
    , member_ssn
    , subscriber_ssn
    , refills
    , daw_indicator
    , rx_flag
    , dispensing_provider_npi
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from {{ source('tuva_historical_select_health','pharmacy_claim') }}
