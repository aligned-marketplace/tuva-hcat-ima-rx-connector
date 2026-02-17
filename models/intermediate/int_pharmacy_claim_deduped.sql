with adjusted_claims as (

    select
          claim_id
        , member_id
        , payer
        , plan
        , prescribing_provider_npi
        , dispensing_provider_npi
        , dispensing_date
        , ndc_code
        , quantity
        , days_supply
        , refills
        , paid_date
        , paid_amount
        , copayment_amount
        , data_source
        , file_name
        , file_date
        , ingest_datetime
    from {{ ref('int_pharmacy_claim_adr') }}
    where row_num = 1

)

, add_fields as (

    select
          claim_id
        , 1 as claim_line_number
        , member_id as person_id
        , member_id
        , payer
        , plan
        , prescribing_provider_npi
        , dispensing_provider_npi
        , dispensing_date
        , ndc_code
        , quantity
        , days_supply
        , refills
        , paid_date
        , paid_amount
        , paid_amount + copayment_amount as allowed_amount
        , null as charge_amount
        , null as coinsurance_amount
        , copayment_amount
        , null as deductible_amount
        , null as in_network_flag
        , data_source
        , file_name
        , file_date
        , ingest_datetime
    from adjusted_claims

)

, data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(claim_line_number as integer) as claim_line_number
        , cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
        , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
        , cast(dispensing_date as date) as dispensing_date
        , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
        , cast(quantity as integer) as quantity
        , cast(days_supply as integer) as days_supply
        , cast(refills as integer) as refills
        , cast(paid_date as date) as paid_date
        , cast(paid_amount as numeric(38,2)) as paid_amount
        , cast(allowed_amount as numeric(38,2)) as allowed_amount
        , cast(charge_amount as numeric(38,2)) as charge_amount
        , cast(coinsurance_amount as numeric(38,2)) as coinsurance_amount
        , cast(copayment_amount as numeric(38,2)) as copayment_amount
        , cast(deductible_amount as numeric(38,2)) as deductible_amount
        , cast(in_network_flag as integer) as in_network_flag
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(file_date as {{ dbt.type_string() }}) as file_date
        , cast(ingest_datetime as datetime) as ingest_datetime
    from add_fields

)

select
      claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , plan
    , prescribing_provider_npi
    , dispensing_provider_npi
    , dispensing_date
    , ndc_code
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , in_network_flag
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from data_types
