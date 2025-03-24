with tmp_order as (
    select
        r.id as restaurant_id,
        r.restaurant_name as restaurant_name,
        t.date as settlement_date,
        count(distinct o.id) as orders_count,
        sum(f.total_sum) as orders_total_sum,
        sum(f.bonus_payment) as orders_bonus_payment_sum,
        sum(f.bonus_grant) as orders_bonus_granted_sum
    from dds.fct_product_sales as f
    inner join dds.dm_orders as o
      on f.order_id = o.id
    inner join dds.dm_timestamps as t
      on t.id = o.timestamp_id
    inner join dds.dm_restaurants as r
      on r.id = o.restaurant_id
    where o.order_status = 'CLOSED'
    group by r.id, r.restaurant_name, t.date
)
insert into cdm.dm_settlement_report(
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
select
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    s.orders_total_sum * 0.25 as order_processing_fee,
    s.orders_total_sum - s.orders_total_sum * 0.25 - s.orders_bonus_payment_sum as restaurant_reward_sum
from tmp_order as s
on conflict (restaurant_id, settlement_date) do update
set
    orders_count = excluded.orders_count,
    orders_total_sum = excluded.orders_total_sum,
    orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
    orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
    order_processing_fee = excluded.order_processing_fee,
    restaurant_reward_sum = excluded.restaurant_reward_sum;