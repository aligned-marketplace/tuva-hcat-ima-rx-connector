with staged_data as (

    select *, row_number() over (
        partition by
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
        order by ingest_datetime desc
      ) as row_num
    from {{ ref('stg_pharmacy_claim') }}
)

, get_first_ingest_datetime as (
    select
        claim_id
        , min(ingest_datetime) as first_ingest_datetime
    from staged_data
    group by claim_id
)

/* source fields not mapped are commented out */
, staged_data_deduped as (

    select
        /*group_number*/
        /*, group_division*/
        /*, group_division_name*/
        /*, employee_id*/
          member_id
        /*, member_zip_code*/
        /*, claimant_relationship*/
        /*, claimant_gender*/
        /*, claimant_dob*/
        , prescribing_provider_npi
        /*, prescribing_physician_name*/
        /*, prescribing_physician_zip*/
        /*, prescribing_physician_tin*/
        /*, pharmacy_nabp*/
        /*, pharmacy_name*/
        /*, pharmacy_zip*/
        , sd.claim_id
        /*, date_prescribed*/
        , dispensing_date
        , paid_date
        /*, drug_name*/
        , ndc_code
        /*, formulary_non_formulary*/
        /*, tier*/
        /*, compound_code*/
        , quantity
        , days_supply
        /*, drug_type*/
        /*, retail_mail_order*/
        /*, ingredient_cost*/
        /*, dispensing_fee*/
        /*, sales_tax*/
        , copayment_amount
        , paid_amount
        /*, member_first_name*/
        /*, member_last_name*/
        /*, member_ssn*/
        /*, subscriber_ssn*/
        , refills
        /*, daw_indicator*/
        /*, rx_flag*/
        , dispensing_provider_npi
        , data_source
        , file_name
        , file_date
        , ingest_datetime
        , first_ingest_datetime
    from staged_data as sd
    left join get_first_ingest_datetime as gfid
    on sd.claim_id = gfid.claim_id
    where row_num = 1
    and member_id is not null /* needed to filter out bad records */

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
        , staged_data_deduped.member_id
        , staged_data_deduped.prescribing_provider_npi
        , staged_data_deduped.dispensing_provider_npi
        , staged_data_deduped.dispensing_date
        , staged_data_deduped.ndc_code
        , staged_data_deduped.quantity
        , staged_data_deduped.days_supply
        , staged_data_deduped.refills
        , staged_data_deduped.paid_date
        , staged_data_deduped.paid_amount
        , staged_data_deduped.copayment_amount
        , staged_data_deduped.data_source
        , staged_data_deduped.file_name
        , staged_data_deduped.file_date
        , staged_data_deduped.first_ingest_datetime as ingest_datetime
    from staged_data_deduped
        left join distinct_eligibility
            on staged_data_deduped.member_id = distinct_eligibility.member_id
            and staged_data_deduped.paid_date
                between distinct_eligibility.enrollment_start_date
                and distinct_eligibility.enrollment_end_date

)

/*
    Here we apply adjustment logic by grouping duplicate sets of claim lines by
    claim id sum the amounts.
*/
, claim_line_totals as (

    select
          claim_id
        , sum(quantity) as sum_quantity
        , sum(days_supply) as sum_days_supply
        , sum(paid_amount) as sum_paid_amount
        , sum(copayment_amount) as sum_copayment_amount
    from mapping
    group by
          claim_id

)

/*
    Next, we use row number to order the claim line sets to get the latest
    version of the claim line.
*/
, claim_line_ordered as (

    select
          mapping.claim_id
        , mapping.member_id
        , mapping.payer
        , mapping.plan
        , mapping.prescribing_provider_npi
        , mapping.dispensing_provider_npi
        , mapping.dispensing_date
        , mapping.ndc_code
        , claim_line_totals.sum_quantity as quantity
        , claim_line_totals.sum_days_supply as days_supply
        , mapping.refills
        , mapping.paid_date
        , claim_line_totals.sum_paid_amount as paid_amount
        , claim_line_totals.sum_copayment_amount as copayment_amount
        , mapping.data_source
        , mapping.file_name
        , mapping.file_date
        , mapping.ingest_datetime
        , row_number() over (
            partition by mapping.claim_id
            order by mapping.paid_date desc
          ) as row_num
    from mapping
        left join claim_line_totals
            on mapping.claim_id = claim_line_totals.claim_id

)

, data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(plan as {{ dbt.type_string() }}) as plan
        , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
        , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
        , date(dispensing_date) as dispensing_date
        , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
        , cast(quantity as numeric(38,2)) as quantity
        , cast(days_supply as integer) as days_supply
        , cast(refills as integer) as refills
        , date(paid_date) as paid_date
        , cast(paid_amount as numeric(38,2)) as paid_amount
        , cast(copayment_amount as numeric(38,2)) as copayment_amount
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(file_date as date) as file_date
        , cast(ingest_datetime as datetime) as ingest_datetime
        , cast(row_num as integer) as row_num
    from claim_line_ordered

)

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
    , row_num
from data_types
