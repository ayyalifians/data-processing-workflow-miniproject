{{ config(materialized='table') }}

SELECT DISTINCT
    product_category
FROM {{ ref('fct_orders') }}
WHERE product_category IS NOT NULL
