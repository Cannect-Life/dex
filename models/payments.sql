{{
    config(
        materialized="table"
    )
}}

with

__pagarme_transactions_cte as
(
    select distinct
        cast(amount as decimal)/100.0 as total_transaction_amount,
        cast(paid_amount as decimal)/100.0 as total_transaction_paid_amount,
        cast(refunded_amount as decimal)/100.0 as total_transaction_refunded_amount,
        cast(json_extract_scalar(customer, '$.email') as varchar) as customer_email,
        cast(json_extract_scalar(item, '$.title') as varchar) as item_title,
        cast(json_extract_scalar(item, '$.unit_price') as decimal)/100.0 as item_unit_price,
        cast(json_extract_scalar(item, '$.quantity') as int) as item_quantity
    from 
        {{ source('dex-dsm-landing', 'pagarme_transactions') }}
        left join unnest(CAST(json_parse(items) AS array<json>)) i(item) on true
    where
        items != '[]'
        -- and json_extract_scalar(customer, '$.email') = 'ricardo.Masumoto@gmail.com'
)

select
    *
from
    __pagarme_transactions_cte