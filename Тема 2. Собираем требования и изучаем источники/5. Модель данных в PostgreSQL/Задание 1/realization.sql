select distinct product_payment::json->>'product_name' as product_name
from (
	select
		product_payments::json->> 1 as product_payment
	from (
		select
			event_value::json->>'product_payments' as product_payments
		from public.outbox o
		where (event_value::json->>'product_payments') is not null
	) a
	where product_payments::json->> 1 is not null
) b
;