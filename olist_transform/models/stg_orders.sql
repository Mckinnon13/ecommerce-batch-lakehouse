{{ config(materialized='view') }}

with source as (
    select * from {{ source('olist_raw', 'orders') }}
)

select
    -- Primary Key
    order_id,
    
    -- Foreign Keys
    customer_id,
    
    -- Timestamps (Casting them from strings to actual timestamps)
    cast(order_purchase_timestamp as timestamp) as purchase_at,
    cast(order_delivered_customer_date as timestamp) as delivered_at,
    
    -- Dimensions
    order_status

from source