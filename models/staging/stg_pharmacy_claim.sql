with source_data as (

    select *
    from {{ source('ima_rx', 'ima_hcat_rx_analysis_by_membe') }}
    where CLAIM_ID is not null

)

select
      cast(CLAIM_ID as {{ dbt.type_string() }})                    as claim_id
    , cast(CLAIM_LINE_SEQUENCE_ID as {{ dbt.type_string() }})      as claim_line_sequence_id
    , cast(STATUS_CODE as {{ dbt.type_string() }})                 as claim_status
    , cast(ADJUSTMENT_CODE as {{ dbt.type_string() }})             as adjustment_code
    , cast(INDIVIDUAL_ID as {{ dbt.type_string() }})               as member_id
    , cast(RELATIONSHIP as {{ dbt.type_string() }})                as dependent_code
    , cast(FULL_NAME as {{ dbt.type_string() }})                   as full_name
    , cast(FIRST_NAME as {{ dbt.type_string() }})                  as first_name
    , cast(LAST_NAME as {{ dbt.type_string() }})                   as last_name
    , cast(DATE_OF_BIRTH as {{ dbt.type_string() }})               as date_of_birth
    , cast(GENDER as {{ dbt.type_string() }})                      as gender
    , cast(STATE as {{ dbt.type_string() }})                       as state
    , cast(EMPLOYER_NAME as {{ dbt.type_string() }})               as employer_name
    , cast(CARRIER_MEMBER_NAME as {{ dbt.type_string() }})         as carrier_member_name
    , cast(CARRIER_PRODUCT_DESCRIPTION as {{ dbt.type_string() }}) as carrier_product_description
    , cast(SERVICE_DATE as {{ dbt.type_string() }})                as service_date
    , cast(PAID_DATE as {{ dbt.type_string() }})                   as paid_date
    , cast(INVOICE_ as {{ dbt.type_string() }})                    as invoice_number
    , cast(INVOICE_DATE as {{ dbt.type_string() }})                as invoice_date
    , cast(NDC_CODE as {{ dbt.type_string() }})                    as ndc_code
    , cast(DRUG_NAME as {{ dbt.type_string() }})                   as drug_name
    , cast(DRUG_STRENGTH as {{ dbt.type_string() }})               as drug_strength
    , cast(PACKAGING_DESCRIPTION as {{ dbt.type_string() }})       as packaging_description
    , cast(RX_VENDOR as {{ dbt.type_string() }})                   as rx_vendor
    , cast(QUANTITY as numeric(38,2))                              as quantity
    , cast(DAYS_SUPPLY as numeric(38,2))                           as days_supply
    , cast(ORIGINAL_COST as numeric(38,2))                         as original_cost
    , cast(SAVINGS as numeric(38,2))                               as savings
    , cast(ACQUISITION_COST as numeric(38,2))                      as acquisition_cost
    , cast(BILLED_AMOUNT as numeric(38,2))                         as billed_amount
    , cast(DIG_SERVICE_FEE as numeric(38,2))                       as dig_service_fee
    , cast(WAIVED_REBATES as numeric(38,2))                        as waived_rebates
    , cast(SAVINGS_TO_CLIENT as numeric(38,2))                     as savings_to_client
    , cast(NET_SAVINGS_TO_CLIENTS as numeric(38,2))                as net_savings_to_clients
    , cast(_FILE as {{ dbt.type_string() }})                       as file_name
    , cast(_MODIFIED as date)                                      as file_date
    , cast(_FIVETRAN_SYNCED as timestamp)                          as ingest_datetime
from source_data