with subs_history as (
select trpl_id, subs_id
from subs_history@ktkdb2
where last_day(to_date('01.01.2020') + 84599/84600) between stime and etime
--and trpl_id = 101--in (38,101) --in (101, 58, 46, 75)
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
    case when sum_this_month = 0 then 0 else 1 end active_this_month_home,
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
        case when trpl_desc = 'B2C' then sum(active_this_month) else sum(active_this_month_home) end active,
        sum(stable) stable, sum(churn) churn, sum(reactivation) reactivation,
        sum(case when abon_type = 'Новые активации' then 1 else 0 end) new_act
from c
group by trpl_name, trpl_desc
),
traffic as (
select /*parallel (8)*/
tp.trpl_name,
s.ext_id s_ext_id,
s.serv_name,
ct.ext_id ct_ext_id,
--s.stype_id,
c.type traf_type,
--c.dim_subs_id,
sum(c.minuts) minuts,
sum(c.mmt_minuts) mmt_minuts,
sum(case when c.wovat_$ > 0 then (c.minuts) else 0 end) minuts_$,
sum(c.duration) duration,
sum(case when c.wovat_$ > 0 then (c.duration) else 0 end) duration_$,
sum(c.vol_in + c.vol_out)/1024/1024 dtraf,
sum(case when c.wovat_$ > 0 then (c.vol_in + c.vol_out)/1024/1024 else 0 end) dtraf_$,
sum(c.wovat_$) wovat_$
from ktk_dwh.FCT_CALL_CHARGE c
join ktk_dwh.dim_service s on c.dim_serv_id = s.dim_serv_id
join ktk_dwh.dim_subscriber sc on c.dim_subs_id = sc.dim_subs_id
join ktk_dwh.dim_tariff_plan tp on c.dim_trpl_id = tp.dim_trpl_id and tp.trpl_desc = 'B2C'--on c.dim_subs_id = sc.dim_subs_id
join ktk_dwh.dim_clch_type ct on c.dim_clch_id = ct.dim_clch_id --and ct.ext_code = 'CALL_TYPE'  --on c.dim_clch_id = ct.dim_clch_id
where sc.client_type_name in ('Абонент B2C', 'Партнер', 'VIP', 'Абонент B2C ( fix )')
--and s.stype_id = 1
--and c.time_id = to_date('01.08.2023', 'DD.MM.YYYY')
and c.time_id between to_date('01.01.2020', 'DD.MM.YYYY') and last_day(to_date('01.01.2020') + 84599/84600)
--and tp.ext_id = 101--in (101, 58, 46, 75)
--and s.ext_id = 2
--and c.type = 2
--and ct.ext_id in (6, 7)
--and c.dim_subs_id = 2555809261
group by tp.trpl_name ,s.ext_id, s.serv_name, ct.ext_id, c.type
)
select * from (
select 1 num, trpl_name, 'Активная база' type, active col
from d
union
select 2 num, trpl_name, 'Стабильная база' type, stable col
from d
union
select 3 num, trpl_name, 'Активации' type, new_act col
from d
union
select 4 num, trpl_name, 'Отток' type, churn col
from d
union
select 5 num, trpl_name, 'Реактивации' type, reactivation col
from d
union
select 6 num, trpl_name, 'ARPU Total' type, case when active != 0 then sum_this_month / active else 0 end col
from d
union
select 7 num, trpl_name, 'Доходы' type, sum_this_month col
from d
union
select 8 num, trpl_name, '   ' type, null col
from d
union
select 8 + 1 num, trpl_name, 'Доход абонентская плата, руб' type, sum(wovat_$) col
from traffic
where trpl_name = serv_name
group by 8 + 1, trpl_name, 'Доход абонентская плата, руб'
union
select 8 + 2 num, trpl_name, 'Доход от опций голос, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1259, 1260, 1411, 1383, 1384, 1385, 1849, 1850, 1330)
group by 8 + 2, trpl_name, 'Доход от опций голос, руб'
union
select 8 + 3 num, trpl_name, 'Доход от опций интернет, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1423, 1421, 1420, 1425, 1449, 1558, 1854, 1557, 1261, 1277, 1296, 1855, 1295, 1352, 1354, 1867, 1856, 1353, 1262, 1374)
group by 8 + 3, trpl_name, 'Доход от опций интернет, руб'
union
select 8 + 4 num, trpl_name, 'Доход от опций прочее (инет, голос, смс), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1428, 1413, 1366, 1803, 1512, 1515)
group by 8 + 4, trpl_name, 'Доход от опций прочее (инет, голос, смс), руб'
union
select 8 + 5 num, trpl_name, 'Доход голос сверх пакета внутри сети, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (2, 114, 131)
group by 8 + 5, trpl_name, 'Доход голос сверх пакета внутри сети, руб'
union
select 8 + 6 num, trpl_name, 'Доход голос сверх пакета моб Крым и КК, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (1, 100, 102, 108, 109, 115, 116, 118, 126)
group by 8 + 6, trpl_name, 'Доход голос сверх пакета моб Крым и КК, руб'
union
select 8 + 7 num, trpl_name, 'Доход голос сверх пакета Россия, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (4, 104, 107, 117, 125)
group by 8 + 7, trpl_name, 'Доход голос сверх пакета Россия, руб'
union
select 8 + 8 num, trpl_name, 'Доход голос МН, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (5, 119, 120, 121)
group by 8 + 8, trpl_name, 'Доход голос МН, руб'
union
select 8 + 9 num, trpl_name, 'Доход инет сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1244, 1410, 1285, 1913)
group by 8 + 9, trpl_name, 'Доход инет сверх пакета, руб'
union
select 8 + 10 num, trpl_name, 'Доход смс внутри сеть сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id = 2
group by 8 + 10, trpl_name, 'Доход смс внутри сеть сверх пакета, руб'
union
select 8 + 11 num, trpl_name, 'Доход смс другие операторы сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id != 2
group by 8 + 11, trpl_name, 'Доход смс другие операторы сверх пакета, руб'
union
select 8 + 11 num, trpl_name, 'Доход смс другие операторы сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id not in (2, 5)
group by 8 + 11, trpl_name, 'Доход смс другие операторы сверх пакета, руб'
union
select 8 + 12 num, trpl_name, 'Доход м/н смс, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id = 5
group by 8 + 12, trpl_name, 'Доход м/н смс, руб'
union
select 8 + 13 num, trpl_name, 'Доход от опций в нацроуминге (абонплата), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1406, 1284, 1912)
group by 8 + 13, trpl_name, 'Доход от опций в нацроуминге (абонплата), руб'
union
select 8 + 14 num, trpl_name, 'Доход от опций в нацроуминге (интернет), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1285, 1410, 1913, 1757, 1406, 1284, 1912, 1892, 1896, 1889, 1899, 1893, 1897, 1890, 1894, 1898, 1900, 1895, 1888, 1891)
group by 8 + 14, trpl_name, 'Доход от опций в нацроуминге (интернет), руб'
union
select 8 + 15 num, trpl_name, 'Доход от опций в нацроуминге (смс), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1409, 1288, 1916)
group by 8 + 15, trpl_name, 'Доход от опций в нацроуминге (смс), руб'
union
select 8 + 16 num, trpl_name, 'Доход от опций в нацроуминге (голос), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1287, 1407, 1915)
group by 8 + 16, trpl_name, 'Доход от опций в нацроуминге (голос), руб'
union
select 8 + 20 num, trpl_name, 'Доход от опций в м/н роуминге , руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1418
group by 8 + 20, trpl_name, 'Доход от опций в м/н роуминге , руб'
union
select 8 + 21 num, trpl_name, 'Доход сохранение номера, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1274
group by 8 + 21, trpl_name, 'Доход сохранение номера, руб'
union
select 8 + 22 num, trpl_name, 'Доход от продажи красивых номеров, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1779, 1783, 1778, 1781, 1784, 1780, 1782, 1785)
group by 8 + 22, trpl_name, 'Доход от продажи красивых номеров, руб'
union
select 8 + 23 num, trpl_name, 'Доход итого, руб' type, sum(wovat_$) col
from traffic
group by 8 + 23, trpl_name, 'Доход итого, руб'
union
select 8 + 24 num, trpl_name, '    ' type, null col
from traffic
union
select 8 + 24 + 1 num, trpl_name, 'Входящий голос внутрисеть, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1256, 1408, 1286, 1914)
and ct_ext_id = 7
--and stype_id = 1
group by 8 + 24 + 1, trpl_name, 'Входящий голос внутрисеть, мин'
union
select 8 + 24 + 2 num, trpl_name, 'Входящий голос другие операторы, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1256, 1408, 1286, 1914)
and ct_ext_id != 7
--and stype_id = 1
group by 8 + 24 + 2, trpl_name, 'Входящий голос другие операторы, мин'
union
select 8 + 24 + 3 num, trpl_name, 'Исходящий голос всего, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
--and stype_id = 1
group by 8 + 24 + 3, trpl_name, 'Исходящий голос всего, мин'
union
select 8 + 24 + 4 num, trpl_name, 'Исходящий голос (нетарифицируемый  внутри сети), мин' type, sum(case when minuts is null then (duration - duration_$) / 60 else minuts - minuts_$ end) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (2, 114, 131)
--and stype_id = 1
group by 8 + 24 + 4, trpl_name, 'Исходящий голос (нетарифицируемый  внутри сети), мин'
union
select 8 + 24 + 5 num, trpl_name, 'Исходящий голос (тарифицируемый  внутри сети), мин' type, sum(nvl(minuts_$, duration_$ / 60)) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (2, 114, 131)
--and stype_id = 1
group by 8 + 24 + 5, trpl_name, 'Исходящий голос (тарифицируемый  внутри сети), мин'
union
select 8 + 24 + 6 num, trpl_name, 'Исходящий голос (нетарифицируемый направление моб.Крым и КК), мин' type, sum(case when minuts is null then (duration - duration_$) / 60 else minuts - minuts_$ end) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (1, 100, 102, 108, 109, 115, 116, 118, 126)
--and stype_id = 1
group by 8 + 24 + 6, trpl_name, 'Исходящий голос (нетарифицируемый направление моб.Крым и КК), мин'
union
select 8 + 24 + 7 num, trpl_name, 'Исходящий голос (тарифицируемый направление моб.Крым и КК), мин' type, sum(nvl(minuts_$, duration_$ / 60)) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (1, 100, 102, 108, 109, 115, 116, 118, 126)
--and stype_id = 1
group by 8 + 24 + 7, trpl_name, 'Исходящий голос (тарифицируемый направление моб.Крым и КК), мин'
union
select 8 + 24 + 8 num, trpl_name, 'Исходящий голос Россия, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (4, 104, 107, 117, 125)
--and stype_id = 1
group by 8 + 24 + 8, trpl_name, 'Исходящий голос Россия, мин'
union
select 8 + 24 + 9 num, trpl_name, 'Исходящий голос МН, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (5, 119, 120, 121)
--and stype_id = 1
group by 8 + 24 + 9, trpl_name, 'Исходящий голос МН, мин'
union
select 8 + 24 + 10 num, trpl_name, 'Исходящий голос на 8800 и короткие номера, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1, 1287, 1407, 1915)
and ct_ext_id in (141, 134, 135, 113)
--and stype_id = 1
group by 8 + 24 + 10, trpl_name, 'Исходящий голос на 8800 и короткие номера, мин'
union
select 8 + 24 + 11 num, trpl_name, 'Интернет трафик (нетарифицируемый), Мб' type, sum(dtraf - dtraf_$) col
from traffic
where s_ext_id in (1244, 1410, 1285, 1913) -- 11502957,6984
--and stype_id = 1
group by 8 + 24 + 11, trpl_name, 'Интернет трафик (нетарифицируемый), Мб'
union
select 8 + 24 + 12 num, trpl_name, 'Интернет трафик (тарифицируемый), Мб' type, sum(dtraf_$) col
from traffic
where s_ext_id in (1244, 1410, 1285, 1913) -- 11502957,6984
--and stype_id = 1
group by 8 + 24 + 12, trpl_name, 'Интернет трафик (тарифицируемый), Мб'
union
select 8 + 24 + 13 num, trpl_name, 'Входящие смс' type, sum(duration) col
from traffic
where s_ext_id in (2, 1409, 1288, 1916)
and ct_ext_id in (6, 7)
--and stype_id = 1
group by 8 + 24 + 13, trpl_name, 'Входящие смс'
union
select 8 + 24 + 14 num, trpl_name, 'Исходящие смс внутрисеть' type, sum(duration) col
from traffic
where s_ext_id = 2
and ct_ext_id = 2
--and stype_id = 1
group by 8 + 24 + 14, trpl_name, 'Исходящие смс внутрисеть'
union
select 8 + 24 + 15 num, trpl_name, 'Исходящие смс другие операторы' type, sum(duration) col
from traffic
where s_ext_id = 2
and ct_ext_id not in (2, 5, 6, 7)
--and stype_id = 1
group by 8 + 24 + 15, trpl_name, 'Исходящие смс другие операторы'
union
select 8 + 24 + 16 num, trpl_name, 'Исходящие смс м/н' type, sum(duration) col
from traffic
where s_ext_id = 2
and ct_ext_id = 5
--and stype_id = 1
group by 8 + 24 + 16, trpl_name, 'Исходящие смс м/н'
union
select 8 + 24 + 17 num, trpl_name, 'Исходящие смс в нацроуминге внутрисеть' type, sum(duration) col
from traffic
where s_ext_id in (1409, 1288, 1916)
and ct_ext_id = 2
--and stype_id = 1
group by 8 + 24 + 17, trpl_name, 'Исходящие смс в нацроуминге внутрисеть'
union
select 8 + 24 + 18 num, trpl_name, 'Исходящие смс в нацроуминге другие операторы' type, sum(duration) col
from traffic
where s_ext_id in (1409, 1288, 1916)
and ct_ext_id not in (2, 5, 6, 7)
--and stype_id = 1
group by 8 + 24 + 18, trpl_name, 'Исходящие смс в нацроуминге другие операторы'
union
select 8 + 24 + 18 num, trpl_name, 'Исходящие смс в нацроуминге м/н' type, sum(duration) col
from traffic
where s_ext_id in (1409, 1288, 1916)
and ct_ext_id = 5
--and stype_id = 1
group by 8 + 24 + 18, trpl_name, 'Исходящие смс в нацроуминге м/н'
)
order by 1, 2
