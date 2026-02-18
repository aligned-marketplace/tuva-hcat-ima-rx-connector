with adr as (

    select *
    from {{ ref('int_pharmacy_claim_adr') }}
    where row_num = 1

)

select
      cast(claim_id as {{ dbt.type_string() }})                    as claim_id
    , cast(1 as integer)                                           as claim_line_number
    , cast(member_id as {{ dbt.type_string() }})                   as person_id
    , cast(member_id as {{ dbt.type_string() }})                   as member_id
    , cast(payer as {{ dbt.type_string() }})                       as payer
    , cast(plan as {{ dbt.type_string() }})                        as plan
    , cast(prescribing_provider_npi as {{ dbt.type_string() }})    as prescribing_provider_npi
    , cast(dispensing_provider_npi as {{ dbt.type_string() }})     as dispensing_provider_npi
    , cast(dispensing_date as date)                                as dispensing_date
    , cast(ndc_code as {{ dbt.type_string() }})                    as ndc_code
    , cast(quantity as integer)                                    as quantity
    , cast(days_supply as integer)                                 as days_supply
    , cast(null as integer)                                        as refills
    , cast(paid_date as date)                                      as paid_date
    , cast(paid_amount as numeric(38,2))                           as paid_amount
    , cast(paid_amount + coinsurance_amount as numeric(38,2))      as allowed_amount
    , cast(null as numeric(38,2))                                  as charge_amount
    , cast(coinsurance_amount as numeric(38,2))                    as coinsurance_amount
    , cast(null as numeric(38,2))                                  as copayment_amount
    , cast(null as numeric(38,2))                                  as deductible_amount
    , cast(null as integer)                                        as in_network_flag
    , cast(data_source as {{ dbt.type_string() }})                 as data_source
    , cast(file_name as {{ dbt.type_string() }})                   as file_name
    , cast(file_date as date)                                      as file_date
    , cast(ingest_datetime as datetime)                            as ingest_datetime
from adr