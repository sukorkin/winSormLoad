with subs_history as (
select trpl_id, subs_id
from subs_history@ktkdb2
where last_day(to_date('01.01.2020') + 84599/84600) between stime and etime
and trpl_id in (101, 58, 46, 75)--=101--in (38,101)
),
subscriber as (
select dim_subs_id, ext_id, activation_date, sh.trpl_id
from ktk_dwh.dim_subscriber s
    inner join subs_history sh on s.ext_id = sh.subs_id
        and s.activation_date < add_months(to_date('01.01.2020'), 1)
    inner join reports.trpl_desc td on sh.trpl_id = td.trpl_id and trpl_segment = 'B2C'
where CLIENT_TYPE_NAME in ('Абонент B2C', 'Партнер', 'VIP', 'Абонент B2C ( fix )')
--where lower(s.CLIENT_TYPE_NAME) not like '%тест%' and lower(s.CLIENT_TYPE_NAME) not like '%служебный%'
),
call_charge as (
select dim_subs_id, month_id, DIM_ITDDESC_ID, Wovat_$, stype_id, serv_name, vol_in, vol_out, vol_in+vol_out vol_in_out
from ktk_dwh.FCT_CALL_CHARGE_DAY_AGG ch
    inner join ktk_dwh.dim_times dt on ch.time_id = dt.time_id
        and dt.month_id between add_months(to_date('01.01.2020'), -3) and to_date('01.01.2020')
    inner join reports.trpl_desc td on ch.dim_trpl_id = td.dim_trpl_id and trpl_segment = 'B2C'
    inner join ktk_dwh.dim_client dc on dc.dim_clnt_id = ch.DIM_CLNT_ID
        --and lower(dc.CLIENT_TYPE_NAME) not like '%тест%' and lower(dc.CLIENT_TYPE_NAME) not like '%служебный%'
        and dc.CLIENT_TYPE_NAME in ('Абонент B2C', 'Партнер', 'VIP', 'Абонент B2C ( fix )')
    left join ktk_dwh.dim_service ds on ch.dim_serv_id = ds.dim_serv_id
),
a as (
select/*parallel(12)*/ s.ext_id, s.activation_date, s.trpl_id as trpl,
    sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) as sum_this_month,
    sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) churn_loss,
    case when nvl(sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end), 0) < 200 then '0-200'
         when sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) >= 200 and
            sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) < 250 then '200-250'
         when sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) >= 250 and
            sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) < 300 then '250-300'
         when sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) >= 300 and
            sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) < 375 then '300-375'
         when sum(case when month_id = add_months(to_date('01.01.2020'), -2) then Wovat_$ else 0 end) >= 375 then '375 и более' else '?' end as churn_arpu_cat,
    count(case when month_id = to_date('01.01.2020') and stype_id = 1 and lower(serv_name) like '%телефония%' then 1
             when month_id = to_date('01.01.2020') and stype_id = 1 and lower(serv_name) like '%sms%'
          and DIM_ITDDESC_ID = 5074 then 1 else null end) calls_this_month,
          sum(case when month_id = to_date('01.01.2020') then vol_in+vol_out end)/1024/1024 traf_this_month,
    count(case when month_id = add_months(to_date('01.01.2020'), -1) and stype_id = 1 and lower(serv_name) like '%телефония%' then 1
             when month_id = add_months(to_date('01.01.2020'), -1) and stype_id = 1 and lower(serv_name) like '%sms%'
          and DIM_ITDDESC_ID = 5074 then 1 else null end) calls_p_month,
    sum(case when month_id = add_months(to_date('01.01.2020'), -1) then vol_in+vol_out end)/1024/1024 traf_p_month,
    count(case when month_id = add_months(to_date('01.01.2020'), -2) and
        stype_id = 1 and lower(serv_name) like '%телефония%' then 1
             when month_id = add_months(to_date('01.01.2020'), -2) and stype_id = 1 and lower(serv_name) like '%sms%'
          and DIM_ITDDESC_ID = 5074 then 1 else null end) calls_pp_month,
    sum(case when month_id = add_months(to_date('01.01.2020'), -2) then vol_in+vol_out end)/1024/1024 traf_pp_month,
    count(case when month_id = add_months(to_date('01.01.2020'), -3) and
        stype_id = 1 and lower(serv_name) like '%телефония%' then 1
             when month_id = add_months(to_date('01.01.2020'), -3) and stype_id = 1 and lower(serv_name) like '%sms%'
          and DIM_ITDDESC_ID = 5074 then 1 else null end) churn_calls_pp_month,
    sum(case when month_id = add_months(to_date('01.01.2020'), -3) then vol_in+vol_out end)/1024/1024 churn_traf_pp_month,
    case when nvl(sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end), 0) < 200 then '0-200'
         when sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) >= 200 and
            sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) < 250 then '200-250'
         when sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) >= 250 and
            sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) < 300 then '250-300'
         when sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) >= 300 and
            sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) < 375 then '300-375'
         when sum(case when month_id = to_date('01.01.2020') then Wovat_$ else 0 end) >= 375 then '375 и более' else '?' end as arpu_cat
from subscriber s
    left join call_charge ch on s.dim_subs_id = ch.dim_subs_id
group by s.ext_id, s.activation_date, s.trpl_id
),
b as (
select ext_id subs_id, activation_date, trpl,
    nvl(sum_this_month, 0) sum_this_month, arpu_cat, nvl(churn_loss, 0) churn_loss, churn_arpu_cat,
    case when (calls_this_month >= 1 or traf_this_month >= 50)
            and (calls_p_month >= 1 or traf_p_month >= 50)
            and (calls_pp_month >= 1 or traf_pp_month >= 50) then 1
        else 0 end stable,
    case when (calls_p_month >= 1 or traf_p_month >= 50)
        and (calls_pp_month >= 1 or traf_p_month >= 50)
        and (churn_calls_pp_month >= 1 or churn_traf_pp_month >= 50) then 1 else 0 end stable_prev,
    case when calls_this_month >= 1 or traf_this_month >= 50 then 1 else 0 end active_this_month,
    case when calls_p_month >= 1 or traf_p_month >= 50 then 1 else 0 end active_prev_month,
    case when trunc(activation_date, 'mm') = to_date('01.01.2020') then 'Новые активации'
        when case when (calls_this_month >= 1 or traf_this_month >= 50)
            and (calls_p_month >= 1 or traf_p_month >= 50)
            and (calls_pp_month >= 1 or traf_pp_month >= 50) then 1
        else 0 end = 1 then 'Стабильная база' else case when case when calls_this_month >= 1 or traf_this_month >= 50
        then 1 else 0 end = 1 then 'Нестабильная база' else 'Неактивная база' end end abon_type,
    case when case when calls_p_month >= 1 or traf_p_month >= 50 then 1 else 0 end = 1
        and case when calls_this_month >= 1 or traf_this_month >= 50 then 1 else 0 end = 0 then 1 else 0 end churn,
    case when case when calls_p_month >= 1 or traf_p_month >= 50 then 1 else 0 end = 0
        and case when calls_this_month >= 1 or traf_this_month >= 50 then 1 else 0 end = 1 then 1 else 0 end reactivation
from a
where case when trunc(activation_date, 'mm') = to_date('01.01.2020') then 'Новые активации'
    when case when (calls_this_month >= 1 or traf_this_month >= 50)
        and (calls_p_month >= 1 or traf_p_month >= 50)
        and (calls_pp_month >= 1 or traf_pp_month >= 50) then 1
    else 0 end = 1 then 'Стабильная база' else case when case when calls_this_month >= 1 or traf_this_month >= 50
    then 1 else 0 end = 1 then 'Нестабильная база' else 'Неактивная база' end end != 'Неактивная база'
    or case when calls_p_month >= 1 or traf_p_month >= 50 then 1 else 0 end = 1
    or nvl(sum_this_month, 0) != 0
    or ext_id in (select subs_id from reports.new_products)
),
c as (
select b.*, msisdn, tp.trpl_name, tp.trpl_desc
--select tp.trpl_name, abon_type, count(1)--to_char(to_date('01.01.2020'), 'MonthYYYY'), b.*, msisdn, tp.trpl_name, tp.trpl_desc
from b
    left join ktk_dwh.dim_subscriber ds on b.subs_id = ds.ext_id
    join ktk_dwh.dim_tariff_plan tp on tp.ext_id = b.trpl
),
d as (
select trpl_name, sum(sum_this_month) sum_this_month,
        sum(active_this_month) active, sum(stable) stable, sum(churn) churn,
        sum(reactivation) reactivation, sum(case when abon_type = 'Новые активации' then 1 else 0 end) new_act
from c
group by trpl_name
)
select * from (
select 0 num, trpl_name, 'Активная база' type, active col
from d
union
select 1 num, trpl_name, 'Стабильная база' type, stable col
from d
union
select 2 num, trpl_name, 'Активации' type, new_act col
from d
union
select 3 num, trpl_name, 'Отток' type, churn col
from d
union
select 4 num, trpl_name, 'Реактивации' type, reactivation col
from d
union
select 5 num, trpl_name, 'ARPU Total' type, case when active != 0 then sum_this_month / active else 0 end col
from d
union
select 6 num, trpl_name, 'Доходы' type, sum_this_month col
from d
)
order by 1
