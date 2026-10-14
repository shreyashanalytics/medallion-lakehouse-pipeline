{# Silver: typed + enriched view of the Spark-produced Parquet (read from S3). #}

with source as (

    select *
    from read_parquet(
        '{{ var("silver_root") }}/transactions/**/*.parquet',
        hive_partitioning = true
    )

),

typed as (

    select
        transaction_id,
        cast(customer_id as bigint)            as customer_id,
        cast(product_id as bigint)             as product_id,
        cast(quantity as integer)              as quantity,
        cast(unit_price as decimal(12, 2))     as unit_price,
        cast(discount_pct as decimal(5, 2))    as discount_pct,
        status,
        payment_method,
        transaction_ts,
        cast(transaction_date as date)         as transaction_date,
        cast(line_amount as decimal(14, 2))    as net_amount,
        cast(round(quantity * unit_price, 2) as decimal(14, 2)) as gross_amount
    from source

)

select
    *,
    gross_amount - net_amount as discount_amount,
    status = 'completed'      as is_completed
from typed
