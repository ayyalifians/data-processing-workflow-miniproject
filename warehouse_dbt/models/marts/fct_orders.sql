
{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_ts,
    product_category,
    quantity,
    unit_price,
    discount_pct,
    payment_method,
    status,
    gross_amount,
    net_amount
FROM {{ ref('stg_orders') }}
