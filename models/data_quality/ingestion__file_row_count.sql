select
      'IMA Rx' as data_source
    , 'pharmacy' as file_type
    , _FILE as file_name
    , max(_FIVETRAN_SYNCED) as last_modified
    , count(*) as file_row_count
from {{ source('ima_rx','ima_hcat_rx_analysis_by_membe') }}
group by _FILE
