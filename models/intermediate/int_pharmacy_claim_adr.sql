with staged as (

    select *
    from {{ ref('stg_pharmacy_claim') }}

)

, get_first_ingest_datetime as (

    select
          claim_id
        , min(ingest_datetime) as first_ingest_datetime
    from staged
    group by claim_id

)

, claim_line_totals as (

    select
          claim_id
        , sum(quantity)      as sum_quantity
        , sum(days_supply)   as sum_days_supply
        , sum(billed_amount) as sum_paid_amount        -- employer-paid amount
        , 0                  as sum_coinsurance_amount -- no member liability
    from staged
    group by claim_id

)

, ordered as (

    select
          s.claim_id
        , s.claim_line_sequence_id
        , s.claim_status
        , s.adjustment_code
        , concat(s.member_id, s.dependent_code)   as member_id
        , s.ndc_code
        , s.rx_vendor                              as type_of_pharmacy
        , s.service_date                           as dispensing_date
        , s.paid_date
        , clt.sum_quantity                         as quantity
        , clt.sum_days_supply                      as days_supply
        , clt.sum_paid_amount                      as paid_amount
        , clt.sum_coinsurance_amount               as coinsurance_amount
        , null                                     as prescribing_provider_npi
        , null                                     as dispensing_provider_npi
        , 'Health Catalyst'                        as payer
        , s.carrier_product_description            as plan
        , 'IMA Rx'                                 as data_source
        , s.file_name
        , s.file_date
        , gfid.first_ingest_datetime               as ingest_datetime
        , row_number() over (
            partition by s.claim_id
            order by s.paid_date desc
          )                                        as row_num
    from staged as s
    left join claim_line_totals as clt
        on s.claim_id = clt.claim_id
    left join get_first_ingest_datetime as gfid
        on s.claim_id = gfid.claim_id

)

select
      cast(claim_id as {{ dbt.type_string() }})                    as claim_id
    , cast(claim_line_sequence_id as {{ dbt.type_string() }})      as claim_line_sequence_id
    , cast(claim_status as {{ dbt.type_string() }})                as claim_status
    , cast(adjustment_code as {{ dbt.type_string() }})             as adjustment_code
    , cast(member_id as {{ dbt.type_string() }})                   as member_id
    , cast(ndc_code as {{ dbt.type_string() }})                    as ndc_code
    , cast(type_of_pharmacy as {{ dbt.type_string() }})            as type_of_pharmacy
    , date(dispensing_date)                                        as dispensing_date
    , date(paid_date)                                              as paid_date
    , cast(quantity as numeric(38,2))                              as quantity
    , cast(days_supply as integer)                                 as days_supply
    , cast(paid_amount as numeric(38,2))                           as paid_amount
    , cast(coinsurance_amount as numeric(38,2))                    as coinsurance_amount
    , cast(prescribing_provider_npi as {{ dbt.type_string() }})    as prescribing_provider_npi
    , cast(dispensing_provider_npi as {{ dbt.type_string() }})     as dispensing_provider_npi
    , cast(payer as {{ dbt.type_string() }})                       as payer
    , cast(plan as {{ dbt.type_string() }})                        as plan
    , cast(data_source as {{ dbt.type_string() }})                 as data_source
    , cast(file_name as {{ dbt.type_string() }})                   as file_name
    , cast(file_date as date)                                      as file_date
    , cast(ingest_datetime as datetime)                            as ingest_datetime
    , cast(row_num as integer)                                     as row_num
from ordered