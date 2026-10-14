{# Gold fact table. Grain: one row per transaction. #}

select
    t.transaction_id,                                   -- degenerate dimension
    d.date_key,
    c.customer_key,
    p.product_key,
    t.quantity,
    t.unit_price,
    t.discount_pct,
    t.gross_amount,
    t.discount_amount,
    t.net_amount,
    case when t.is_completed then t.net_amount else 0 end as recognized_revenue,
    t.status,
    t.payment_method,
    t.transaction_ts
from {{ ref('silver_transactions') }} as t
left join {{ ref('dim_date') }}      as d on t.transaction_date = d.date_day
left join {{ ref('dim_customer') }}  as c on t.customer_id = c.customer_id
left join {{ ref('dim_product') }}   as p on t.product_id = p.product_id
