select
    cast(customer_id as bigint) as customer_id,
    first_name,
    last_name,
    email,
    gender,
    cast(age as integer)        as age,
    phone,
    city,
    state,
    country
from read_parquet('{{ var("silver_root") }}/customers/*.parquet')
