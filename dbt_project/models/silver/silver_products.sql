select
    cast(product_id as bigint)         as product_id,
    title,
    category,
    brand,
    cast(price as decimal(10, 2))      as price,
    cast(rating as decimal(3, 2))      as rating,
    cast(stock as integer)             as stock,
    sku
from read_parquet('{{ var("silver_root") }}/products/*.parquet')
