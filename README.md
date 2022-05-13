# Проект 2
Опишите здесь поэтапно ход решения задачи. Вы можете ориентироваться на тот план выполнения проекта, который мы предлагаем в инструкции на платформе.
## shipping_country
```
CREATE TABLE public.shipping_country(
    shipping_country_id serial primary key ,
    shipping_country text,
    shipping_country_base_rate numeric(14,3)
);
insert into public.shipping_country(shipping_country, shipping_country_base_rate)
select distinct shipping_country, shipping_country_base_rate from public.shipping;
```
## shipping_agreement
```
CREATE TABLE public.shipping_agreement(
    agreementid int primary key ,
    agreement_number text,
    agreement_rate numeric(14,2),
    agreement_commission numeric(14,2)
);
insert into public.shipping_agreement(agreementid, agreement_number, agreement_rate, agreement_commission)
select  distinct splitted[1]::bigint, splitted[2]::text, splitted[3]::numeric, splitted[4]::numeric
from (
select regexp_split_to_array(vendor_agreement_description , E'\\:+') as splitted
from public.shipping) t1
order by 1;
```

## shipping_transfer
```
CREATE TABLE public.shipping_transfer (
    transfer_type_id serial  primary key ,
    transfer_type text,
    transfer_model text,
    shipping_transfer_rate  numeric(14,3)
);
insert into public.shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
select splitted[1]::text, splitted[2]::text, shipping_transfer_rate
from (
    select distinct regexp_split_to_array(shipping_transfer_description , E'\\:+') as splitted , shipping_transfer_rate
    from public.shipping) t1
order by 1;
```

## shipping_info
```
CREATE TABLE public.shipping_info (
    shippingid bigint  ,
    vendor_id int,
    payment_amount numeric(14,2),
    shipping_plan_datetime  timestamp,
    transfer_type_id int references public.shipping_transfer(transfer_type_id),
    shipping_country_id int references public.shipping_country(shipping_country_id),
    agreementid int references public.shipping_agreement(agreementid)
);

insert into public.shipping_info(shippingid, vendor_id, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
select s.shippingid, s.vendorid, s.payment_amount, s.shipping_plan_datetime, st.transfer_type_id, sc.shipping_country_id, sa.agreementid
from public.shipping s
join public.shipping_transfer st on concat(st.transfer_type,':',st.transfer_model) = s.shipping_transfer_description
join public.shipping_country sc on sc.shipping_country= s.shipping_country
join public.shipping_agreement sa on sa.agreementid::text = (regexp_split_to_array(s.vendor_agreement_description , E'\\:+'))[1]::text;
```

## shipping_status
```
CREATE TABLE public.shipping_status (
    shippingid bigint,
    status text,
    state text,
    shipping_start_fact_datetime timestamp,
    shipping_end_fact_datetime timestamp
);


insert into public.shipping_status(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)

with state_status as (
select t2.shippingid, t1.state, t1.status
from shipping t1
right join (select shippingid, max(state_datetime) as max_state_datetime
from shipping
group by shippingid) t2 on t1.shippingid = t2.shippingid and t1.state_datetime = t2.max_state_datetime),
    booked_revieved as (
select t1.shippingid, state_datetime as shipping_start_fact_datetim,shipping_end_fact_datetime
from shipping t1
left join (select shippingid, state_datetime as shipping_end_fact_datetime
            from shipping
            where state = 'recieved') t2 on t1.shippingid = t2.shippingid
where state = 'booked')

select ss.shippingid, ss.status, ss.state, br.shipping_start_fact_datetim, br.shipping_end_fact_datetime
from state_status as ss
join booked_revieved as br using(shippingid);
```

## shipping_datamart
```
drop table public.shipping_datamart;
CREATE TABLE public.shipping_datamart (
    shippingid bigint,
    vendorid bigint,
    transfer_type text,
    full_day_at_shipping int,
    is_delay int,
    is_shipping_finish int,
    delay_day_at_shipping int,
    payment_amount numeric(14,2),
    vat numeric(14,6),
    profit numeric(14,6)
);
insert into public.shipping_datamart(shippingid, vendorid, transfer_type, full_day_at_shipping, is_delay, is_shipping_finish, delay_day_at_shipping, payment_amount, vat, profit)
select si.shippingid, si.vendor_id, st.transfer_type,
       date_part('day', age(shipping_end_fact_datetime,shipping_start_fact_datetime)) as full_day_at_shipping,
        CASE
            WHEN shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1
            ELSE 0
        END  as is_delay,

        CASE
            WHEN status = 'finished' THEN 1
            ELSE 0
        END  as is_shipping_finish,

        CASE
            WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime  THEN date_part('day', age(ss.shipping_end_fact_datetime,si.shipping_plan_datetime))
            ELSE 0
        END  as delay_day_at_shipping,
        si.payment_amount,
        si.payment_amount * (sc.shipping_country_base_rate +sa.agreement_rate +st.shipping_transfer_rate) as vat,
        si.payment_amount * sa.agreement_commission as profit

from public.shipping_info si
join public.shipping_transfer st using(transfer_type_id)
join public.shipping_status ss using(shippingid)
join public.shipping_country sc using(shipping_country_id)
join public.shipping_agreement sa using(agreementid);
```
