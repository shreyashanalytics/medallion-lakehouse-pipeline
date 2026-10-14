select
    md5(cast(customer_id as varchar))             as customer_key,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name                as full_name,
    email,
    gender,
    age,
    case
        when age is null then 'unknown'
        when age < 25 then '18-24'
        when age < 35 then '25-34'
        when age < 50 then '35-49'
        else '50+'
    end                                           as age_band,
    city,
    state,
    country
from {{ ref('silver_customers') }}
