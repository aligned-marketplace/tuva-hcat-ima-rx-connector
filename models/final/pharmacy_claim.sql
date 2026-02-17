select * from {{ ref('int_pharmacy_claim_deduped') }}

union all 

select * from {{ ref('int_pharmacy_ima_claim_deduped') }}