{{ config(materialized='table') }}

WITH orders AS (

SELECT *
FROM {{ ref('stg_orders3') }}

),

customers AS (

SELECT *
FROM {{ ref('stg_customers2') }}

),

products AS (

SELECT *
FROM {{ ref('stg_products') }}

),

returns AS (

SELECT *
FROM {{ ref('stg_returns') }}

),

joined AS (

SELECT
o.order_id,
o.order_timestamp,

DATE(o.order_timestamp) AS order_date,
YEAR(o.order_timestamp) AS order_year,
MONTH(o.order_timestamp) AS order_month,

o.customer_id,

CONCAT(c.first_name,' ',c.last_name) AS customer_name,

o.product_id,
p.product_name,
p.category,

o.quantity,
o.price,
o.discount,

o.shipping_info,

c.is_vip,
c.is_newsletter,
c.is_loyal,

p.cost AS product_cost

FROM orders o

LEFT JOIN customers c
ON o.customer_id = c.customer_id

LEFT JOIN products p
ON o.product_id = p.product_id

LEFT JOIN returns r
ON o.order_id = r.order_id
AND o.product_id = r.product_id
WHERE r.product_id IS NULL
-- WHERE NOT EXISTS (
--     SELECT 1
--     FROM returns r
--     WHERE r.order_id = o.order_id
--       AND LOWER(TRIM(r.product_id)) = LOWER(TRIM(o.product_id))
-- )

),

metrics AS (

SELECT
*,

quantity * price AS revenue,

quantity * price * discount AS discount_amount,

(quantity * price) - (quantity * price * discount) AS net_revenue,

(quantity * price) - (quantity * price * discount) - (quantity * product_cost) AS profit

FROM joined
),

aggregated AS (

SELECT
order_id,

collect_set(product_name) AS products_in_order,
collect_set(category) AS categories_in_order

FROM metrics
GROUP BY order_id
)

SELECT
m.*,

{{ calc_margin('profit','net_revenue') }} AS profit_margin,

a.products_in_order,
a.categories_in_order

FROM metrics m
LEFT JOIN aggregated a
ON m.order_id = a.order_id