-- Singular test: any returned row is a failure.
select transaction_id
from {{ ref('fact_sales') }}
where net_amount < 0 or recognized_revenue < 0
