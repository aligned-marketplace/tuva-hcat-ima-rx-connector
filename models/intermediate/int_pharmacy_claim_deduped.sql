with adr as (

    select *
    from {{ ref('int_pharmacy_claim_adr') }}
    where row_num = 1

)

, carrier_lookup as (

    select *
    from {{ ref('int_carrier_eligibility_lookup') }}

)

select
      cast(adr.claim_id as {{ dbt.type_string() }})                    as claim_id
    , cast(1 as integer)                                               as claim_line_number
    , cast(cl.person_id as {{ dbt.type_string() }})                    as person_id
    , cast(adr.member_id as {{ dbt.type_string() }})                   as member_id
    , cast(cl.carrier_data_source as {{ dbt.type_string() }})          as payer
    , cast(cl.plan as {{ dbt.type_string() }})                         as plan
    , cast(adr.prescribing_provider_npi as {{ dbt.type_string() }})    as prescribing_provider_npi
    , cast(adr.dispensing_provider_npi as {{ dbt.type_string() }})     as dispensing_provider_npi
    , cast(adr.dispensing_date as date)                                as dispensing_date
    , cast(adr.ndc_code as {{ dbt.type_string() }})                    as ndc_code
    , cast(adr.quantity as integer)                                    as quantity
    , cast(adr.days_supply as integer)                                 as days_supply
    , cast(null as integer)                                            as refills
    , cast(adr.paid_date as date)                                      as paid_date
    , cast(adr.paid_amount as numeric(38,2))                           as paid_amount
    , cast(adr.paid_amount + adr.coinsurance_amount as numeric(38,2))  as allowed_amount
    , cast(null as numeric(38,2))                                      as charge_amount
    , cast(adr.coinsurance_amount as numeric(38,2))                    as coinsurance_amount
    , cast(null as numeric(38,2))                                      as copayment_amount
    , cast(null as numeric(38,2))                                      as deductible_amount
    , cast(null as integer)                                            as in_network_flag
    , cast(adr.data_source as {{ dbt.type_string() }})                 as data_source
    , cast(adr.file_name as {{ dbt.type_string() }})                   as file_name
    , cast(adr.file_date as date)                                      as file_date
    , cast(adr.ingest_datetime as datetime)                            as ingest_datetime
from adr
left join carrier_lookup as cl
    on adr.member_id = cl.individual_id
