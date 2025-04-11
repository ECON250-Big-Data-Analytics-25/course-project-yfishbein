{{ config(materialized='view') }}

select
order_id,
payment_sequential,
lower(payment_type) as payment_type,
payment_installments,
payment_value,
case 
    when cast(payment_installments as integer) > 1 then true
    else false
end as is_installment_payment,
case 
    when payment_installments > 1 
    then SAFE_DIVIDE(payment_value, payment_installments)
    else payment_value
end as installment_amount,
case when lower(payment_type) = 'credit_card' then true else false end as is_credit_card,
case when lower(payment_type) = 'boleto' then true else false end as is_boleto,
case when lower(payment_type) = 'voucher' then true else false end as is_voucher,
case when lower(payment_type) = 'debit_card' then true else false end as is_debit_card,
from {{ source('yfishbein', 'fp_olist_order_payments_dataset') }}