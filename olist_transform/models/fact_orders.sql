{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchase_at,
    c.city,
    c.state
from orders o
left join customers c on o.customer_id = c.customer_id