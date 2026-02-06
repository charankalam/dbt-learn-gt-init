{% set status_type = ['completed', 'pending', 'canceled'] %}

with orders as 
(
    select  orders.order_id,
            orders.status,
            payments.amount
    from {{ref('stage_orders')}} orders
    left join {{ref('stage_payments')}} payments using (order_id)
  
),

pivoted as
(
    select 
        order_id,
        {% for status in status_type %}
        sum(case when status = '{{ status }}' then amount else 0 end) as {{ status }}_amount {% if not loop.last %},{% endif %} 
        {% endfor %}
    from orders
    group by order_id
)

select * from pivoted   