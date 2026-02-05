

with customers as (

   select * from {{ref('stage_customers')}} 

),

orders as (

    select * from {{ref('stage_orders')}} 

),



customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
        
    from orders

    group by 1

),

customer_payments as (

select order_id, amount 
from {{ref('stage_payments')}}

),



final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
       sum( coalesce(customer_orders.number_of_orders, 0)) as number_of_orders,
       
        sum(customer_payments.amount) as life_time_value

    from customers customers

    left join customer_orders customer_orders using (customer_id)

    left join {{ref('stage_orders')}} stage_orders using (customer_id)

    left join customer_payments customer_payments using (order_id)

    group by 1,2,3,4,5

)

select * from final












