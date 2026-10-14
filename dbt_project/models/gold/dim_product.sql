select
    md5(cast(product_id as varchar))              as product_key,
    product_id,
    title                                         as product_name,
    category,
    brand,
    sku,
    price                                         as list_price,
    rating,
    case
        when price < 20 then 'budget'
        when price < 100 then 'mid'
        else 'premium'
    end                                           as price_tier
from {{ ref('silver_products') }}
