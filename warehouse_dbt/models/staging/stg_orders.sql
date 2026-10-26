
{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_ts AS TIMESTAMPTZ) AS order_ts,
    CAST(product_category AS VARCHAR) AS product_category,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    CAST(discount_pct AS DOUBLE) AS discount_pct,
    CAST(payment_method AS VARCHAR) AS payment_method,
    CAST(status AS VARCHAR) AS status,
    CAST(gross_amount AS DOUBLE) AS gross_amount,
    CAST(net_amount AS DOUBLE) AS net_amount
FROM read_parquet(
    'C:/Users/user/Documents/SDT B PENS 2024/Semester 5/Praktikum Data Processing Workflow/mini-project/data_processing_miniproject/data/processed/transformed_polars.parquet'
)
