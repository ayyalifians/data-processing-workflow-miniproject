{{ config(materialized='table') }}

SELECT
    CAST(order_ts AS DATE) AS order_date,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(gross_amount) AS total_gross_sales,
    SUM(net_amount) AS total_net_sales,
    AVG(net_amount) AS average_net_order_value
FROM {{ ref('fct_orders') }}
GROUP BY CAST(order_ts AS DATE)
