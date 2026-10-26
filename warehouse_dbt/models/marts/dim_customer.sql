{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id
FROM {{ ref('fct_orders') }}
WHERE customer_id IS NOT NULL
