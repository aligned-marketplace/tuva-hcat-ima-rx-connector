select
      'Select Health' data_source
    , 'eligibility' as file_type
    , _file as file_name
    , max(_fivetran_synced) as last_modified
    , count(*) as file_row_count
from {{ source('select_health','eligibility') }}
group by _file

union all

select
      'Select Health' data_source
    , 'medical' as file_type
    , _file as file_name
    , max(_fivetran_synced) as last_modified
    , count(*) as file_row_count
from {{ source('select_health','medical_claim') }}
group by _file

union all

select
      'Select Health' data_source
    , 'pharmacy' as file_type
    , _file as file_name
    , max(_fivetran_synced) as last_modified
    , count(*) as file_row_count
from {{ source('select_health','pharmacy_claim') }}
group by _file