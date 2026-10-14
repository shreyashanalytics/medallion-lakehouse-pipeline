with bounds as (

    select
        min(transaction_date) as first_day,
        max(transaction_date) as last_day
    from {{ ref('silver_transactions') }}

),

days as (

    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as date_day
    from bounds

)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day)                                as year,
    quarter(date_day)                             as quarter,
    month(date_day)                               as month,
    monthname(date_day)                           as month_name,
    dayofweek(date_day)                           as day_of_week,
    dayname(date_day)                             as day_name,
    dayofweek(date_day) in (0, 6)                 as is_weekend
from days
