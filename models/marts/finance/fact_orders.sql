with customer_payment as 
(
    select  customers.customer_id,
        customers.first_name,
        customers.last_name ,
        orders.order_id,
        payments.amount
        
    
    
    
    from {{ref('stage_customers')}} customers

     left join {{ref('stage_orders')}} orders using (customer_id)
     left join {{ref('stage_payments')}} payments using (order_id)
    

)

select * from customer_payment