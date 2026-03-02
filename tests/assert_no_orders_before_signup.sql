-- Assert that no orders were placed before the customer's signup date
select
    o.order_id,
    o.customer_id,
    o.order_at,
    c.signup_at

from {{ ref('stg_shopify__orders') }} as o
inner join {{ ref('stg_shopify__customers') }} as c
    on o.customer_id = c.customer_id
where o.order_at < c.signup_at
