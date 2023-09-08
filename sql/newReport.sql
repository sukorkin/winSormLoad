with subs_history as (
select trpl_id, subs_id
from subs_history@ktkdb2
where last_day(to_date('01.01.2020') + 84599/84600) between stime and etime
--and trpl_id = 7--101--in (38,101) --in (101, 58, 46, 75)
),
subscriber as (
select dim_subs_id, ext_id, activation_date, sh.trpl_id
from ktk_dwh.dim_subscriber s
    inner join subs_history sh on s.ext_id = sh.subs_id
        and s.activation_date < add_months(to_date('01.01.2020'), 1)
    inner join reports.trpl_desc td on sh.trpl_id = td.trpl_id and trpl_segment = 'B2C' and trpl_product = 'Голос'
where CLIENT_TYPE_NAME in ('Абонент B2C', 'Партнер', 'VIP', 'Абонент B2C ( fix )')
--where lower(s.CLIENT_TYPE_NAME) not like '%тест%' and lower(s.CLIENT_TYPE_NAME) not like '%служебный%'
),
call_charge as (
select dim_subs_id, month_id, DIM_ITDDESC_ID, Wovat_$, stype_id, serv_name, vol_in, vol_out, vol_in+vol_out vol_in_out
from ktk_dwh.FCT_CALL_CHARGE_DAY_AGG ch
    inner join ktk_dwh.dim_times dt on ch.time_id = dt.time_id
        and dt.month_id between add_months(to_date('01.01.2020'), -3) and to_date('01.01.2020')
    inner join reports.trpl_desc td on ch.dim_trpl_id = td.dim_trpl_id and trpl_segment = 'B2C' and trpl_product = 'Голос'
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
    case when traf_this_month > 0 then 1 else 0 end active_traf_this_month,
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
        sum(stable) stable, sum(churn) churn, sum(reactivation) reactivation, sum(active_traf_this_month) active_traf,
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
--and tp.ext_id = 7--101--in (101, 58, 46, 75)
--and s.ext_id = 2
--and c.type = 2
--and ct.ext_id in (6, 7)
--and c.dim_subs_id = 2555809261
group by tp.trpl_name, s.ext_id, s.serv_name, ct.ext_id, c.type
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
select 8 num, trpl_name, 'Активная интернет база (пользователи интернета)' type, active_traf col
from d
union
select 9 num, trpl_name, '   ' type, null col
from d
union
select 9 + 1 num, trpl_name, 'Доход абонентская плата, руб' type, sum(wovat_$) col
from traffic
where trpl_name = serv_name
or (trpl_name = 'Волна РНКБ 2020' and s_ext_id in (1426, 1427))
or (trpl_name = 'Волна РНКБ 2021' and s_ext_id in (1509, 1510))
or (trpl_name = 'Волна РНКБ Прайм' and s_ext_id = 1588)
or (trpl_name = 'Сделай сам!' and s_ext_id in (1642, 1641, 1640, 1639))
or (trpl_name = 'Сделай сам! NEW' and s_ext_id in (1813, 1812, 1810, 1811))
or (trpl_name = 'Ветер' and s_ext_id in (1265, 1340, 1341, 1342))

group by 9 + 1, trpl_name, 'Доход абонентская плата, руб'
union
select 9 + 2 num, trpl_name, 'Доход от опций голос, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1259, 1260, 1411, 1383, 1384, 1385, 1849, 1850, 1330)
group by 9 + 2, trpl_name, 'Доход от опций голос, руб'
union
select 9 + 3 num, trpl_name, 'Доход от опций интернет, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1423, 1421, 1420, 1425, 1449, 1558, 1854, 1557, 1261, 1277, 1296, 1855, 1295, 1352, 1353, 1354, 1867, 1856, 1262, 1374)
group by 9 + 3, trpl_name, 'Доход от опций интернет, руб'
union
select 9 + 4 num, trpl_name, 'Доход от опций прочее (инет, голос, смс), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1428, 1413, 1366, 1803, 1512, 1515)
group by 9 + 4, trpl_name, 'Доход от опций прочее (инет, голос, смс), руб'
union
select 9 + 5 num, trpl_name, 'Доход голос сверх пакета внутри сети, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1
and ct_ext_id in (2, 114, 131)
and traf_type = 1
group by 9 + 5, trpl_name, 'Доход голос сверх пакета внутри сети, руб'
union
select 9 + 6 num, trpl_name, 'Доход голос сверх пакета моб Крым и КК, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1
and ct_ext_id in (1, 100, 102, 108, 109, 115, 116, 118, 126)
and traf_type = 1
group by 9 + 6, trpl_name, 'Доход голос сверх пакета моб Крым и КК, руб'
union
select 9 + 7 num, trpl_name, 'Доход голос сверх пакета Россия, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1
and ct_ext_id in (4, 104, 107, 117, 125)
and traf_type = 1
group by 9 + 7, trpl_name, 'Доход голос сверх пакета Россия, руб'
union
select 9 + 8 num, trpl_name, 'Доход голос МН, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1
and ct_ext_id in (5, 119, 120, 121)
and traf_type = 1
group by 9 + 8, trpl_name, 'Доход голос МН, руб'
union
select 9 + 9 num, trpl_name, 'Доход инет сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1244
and traf_type = 1
group by 9 + 9, trpl_name, 'Доход инет сверх пакета, руб'
union
select 9 + 10 num, trpl_name, 'Доход смс внутри сеть сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id = 2
group by 9 + 10, trpl_name, 'Доход смс внутри сеть сверх пакета, руб'
union
select 9 + 11 num, trpl_name, 'Доход смс другие операторы сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id != 2
group by 9 + 11, trpl_name, 'Доход смс другие операторы сверх пакета, руб'
union
select 9 + 12 num, trpl_name, 'Доход смс другие операторы сверх пакета, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id not in (2, 5)
group by 9 + 12, trpl_name, 'Доход смс другие операторы сверх пакета, руб'
union
select 9 + 13 num, trpl_name, 'Доход м/н смс, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 2
and ct_ext_id = 5
group by 9 + 13, trpl_name, 'Доход м/н смс, руб'
union
select 9 + 14 num, trpl_name, 'Доход от опций в нацроуминге (абонплата), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1406, 1284, 1912)
group by 9 + 14, trpl_name, 'Доход от опций в нацроуминге (абонплата), руб'
union
select 9 + 15 num, trpl_name, 'Доход от опций в нацроуминге (интернет), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1285, 1410, 1913, 1757, 1892, 1896, 1889, 1899, 1893, 1897, 1890, 1894, 1898, 1900, 1895, 1888, 1891)
group by 9 + 15, trpl_name, 'Доход от опций в нацроуминге (интернет), руб'
union
select 9 + 16 num, trpl_name, 'Доход от опций в нацроуминге (смс), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1409, 1288, 1916)
group by 9 + 16, trpl_name, 'Доход от опций в нацроуминге (смс), руб'
union
select 9 + 17 num, trpl_name, 'Доход от опций в нацроуминге (голос), руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1287, 1407, 1915)
group by 9 + 17, trpl_name, 'Доход от опций в нацроуминге (голос), руб'
union
select 9 + 18 num, trpl_name, 'Доход инет сверх пакета в нацроуминге, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1244
and traf_type = 2
group by 9 + 18, trpl_name, 'Доход инет сверх пакета в нацроуминге, руб'
union
select 9 + 19 num, trpl_name, 'Доход голос сверх пакета в нацроуминге, руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1
and traf_type = 2
group by 9 + 19, trpl_name, 'Доход голос сверх пакета в нацроуминге, руб'
union
select 9 + 20 num, trpl_name, 'Доход от опций в м/н роуминге , руб' type, sum(wovat_$) col
from traffic
where s_ext_id = 1418
group by 9 + 20, trpl_name, 'Доход от опций в м/н роуминге , руб'
union
select 9 + 21 num, trpl_name, 'Доход другое, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1274, 1297, 1742, 1764, -6, -10)
group by 9 + 21, trpl_name, 'Доход другое, руб'
union
select 9 + 22 num, trpl_name, 'Доход от продажи красивых номеров, руб' type, sum(wovat_$) col
from traffic
where s_ext_id in (1779, 1783, 1778, 1781, 1784, 1780, 1782, 1785, 1279, 1249, 1248, -12, -11)
group by 9 + 22, trpl_name, 'Доход от продажи красивых номеров, руб'
union
select 9 + 23 num, trpl_name, 'Доход итого, руб' type, sum(wovat_$) col
from traffic
group by 9 + 23, trpl_name, 'Доход итого, руб'
union
select 9 + 24 num, trpl_name, '    ' type, null col
from traffic
union
select 9 + 24 + 1 num, trpl_name, 'Входящий голос внутрисеть, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1256, 1408, 1286, 1914)
and ct_ext_id = 7
--and stype_id = 1
group by 9 + 24 + 1, trpl_name, 'Входящий голос внутрисеть, мин'
union
select 9 + 24 + 2 num, trpl_name, 'Входящий голос другие операторы, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1256, 1408, 1286, 1914)
and ct_ext_id != 7
--and stype_id = 1
group by 9 + 24 + 2, trpl_name, 'Входящий голос другие операторы, мин'
union
select 9 + 24 + 3 num, trpl_name, 'Исходящий голос всего, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id = 1
--and stype_id = 1
group by 9 + 24 + 3, trpl_name, 'Исходящий голос всего, мин'
union
select 9 + 24 + 4 num, trpl_name, 'Исходящий голос (нетарифицируемый  внутри сети), мин' type, sum(case when minuts is null then (duration - duration_$) / 60 else minuts - minuts_$ end) col
from traffic
where s_ext_id = 1
and ct_ext_id in (2, 114, 131)
--and stype_id = 1
group by 9 + 24 + 4, trpl_name, 'Исходящий голос (нетарифицируемый  внутри сети), мин'
union
select 9 + 24 + 5 num, trpl_name, 'Исходящий голос (тарифицируемый  внутри сети), мин' type, sum(nvl(minuts_$, duration_$ / 60)) col
from traffic
where s_ext_id = 1
and ct_ext_id in (2, 114, 131)
--and stype_id = 1
group by 9 + 24 + 5, trpl_name, 'Исходящий голос (тарифицируемый  внутри сети), мин'
union
select 9 + 24 + 6 num, trpl_name, 'Исходящий голос (нетарифицируемый направление моб.Крым и КК), мин' type, sum(case when minuts is null then (duration - duration_$) / 60 else minuts - minuts_$ end) col
from traffic
where s_ext_id = 1
and ct_ext_id in (1, 100, 102, 108, 109, 115, 116, 118, 126)
--and stype_id = 1
group by 9 + 24 + 6, trpl_name, 'Исходящий голос (нетарифицируемый направление моб.Крым и КК), мин'
union
select 9 + 24 + 7 num, trpl_name, 'Исходящий голос (тарифицируемый направление моб.Крым и КК), мин' type, sum(nvl(minuts_$, duration_$ / 60)) col
from traffic
where s_ext_id = 1
and ct_ext_id in (1, 100, 102, 108, 109, 115, 116, 118, 126)
--and stype_id = 1
group by 9 + 24 + 7, trpl_name, 'Исходящий голос (тарифицируемый направление моб.Крым и КК), мин'
union
select 9 + 24 + 8 num, trpl_name, 'Исходящий голос нетарифицируемый Россия, мин' type, sum(case when minuts is null then (duration - duration_$) / 60 else minuts - minuts_$ end) col
from traffic
where s_ext_id = 1
and ct_ext_id in (4, 104, 107, 117, 125)
--and stype_id = 1
group by 9 + 24 + 8, trpl_name, 'Исходящий голос нетарифицируемый Россия, мин'
union
select 9 + 24 + 9 num, trpl_name, 'Исходящий голос тарифицируемый Россия, мин' type, sum(nvl(minuts_$, duration_$ / 60)) col
from traffic
where s_ext_id = 1
and ct_ext_id in (4, 104, 107, 117, 125)
--and stype_id = 1
group by 9 + 24 + 9, trpl_name, 'Исходящий голос тарифицируемый Россия, мин'
union
select 9 + 24 + 10 num, trpl_name, 'Исходящий голос МН, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id = 1
and ct_ext_id in (5, 119, 120, 121)
--and stype_id = 1
group by 9 + 24 + 10, trpl_name, 'Исходящий голос МН, мин'
union
select 9 + 24 + 11 num, trpl_name, 'Исходящий голос на 8800 и короткие номера, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id = 1
and ct_ext_id in (141, 134, 135, 113)
--and stype_id = 1
group by 9 + 24 + 11, trpl_name, 'Исходящий голос на 8800 и короткие номера, мин'
union
select 9 + 24 + 12 num, trpl_name, 'Исходящий голос (нетарифицируемый) в нацроуминге, мин' type, sum(case when minuts is null then (duration - duration_$) / 60 else minuts - minuts_$ end) col
from traffic
where s_ext_id in (1287, 1407, 1915)
--and stype_id = 1
group by 9 + 24 + 12, trpl_name, 'Исходящий голос (нетарифицируемый) в нацроуминге, мин'
union
select 9 + 24 + 13 num, trpl_name, 'Исходящий голос (тарифицируемый) в нацроуминге, мин' type, sum(nvl(minuts, duration / 60)) col
from traffic
where s_ext_id in (1287, 1407, 1915)
--and stype_id = 1
group by 9 + 24 + 13, trpl_name, 'Исходящий голос (тарифицируемый) в нацроуминге, мин'
union
select 9 + 24 + 14 num, trpl_name, 'Интернет трафик (нетарифицируемый), Мб' type, sum(dtraf - dtraf_$) col
from traffic
where s_ext_id = 1244
--and stype_id = 1
group by 9 + 24 + 14, trpl_name, 'Интернет трафик (нетарифицируемый), Мб'
union
select 9 + 24 + 15 num, trpl_name, 'Интернет трафик (тарифицируемый), Мб' type, sum(dtraf_$) col
from traffic
where s_ext_id = 1244
--and stype_id = 1
group by 9 + 24 + 15, trpl_name, 'Интернет трафик (тарифицируемый), Мб'
union
select 9 + 24 + 16 num, trpl_name, 'Интернет трафик (нетарифицируемый) в нацроуминге, Мб' type, sum(dtraf - dtraf_$) col
from traffic
where s_ext_id in (1410, 1285, 1913)
--and stype_id = 1
group by 9 + 24 + 16, trpl_name, 'Интернет трафик (нетарифицируемый) в нацроуминге, Мб'
union
select 9 + 24 + 17 num, trpl_name, 'Интернет трафик (тарифицируемый) в нацроуминге, Мб' type, sum(dtraf_$) col
from traffic
where s_ext_id in (1410, 1285, 1913)
--and stype_id = 1
group by 9 + 24 + 17, trpl_name, 'Интернет трафик (тарифицируемый) в нацроуминге, Мб'
union
select 9 + 24 + 18 num, trpl_name, 'Входящие смс' type, sum(duration) col
from traffic
where s_ext_id in (2, 1409, 1288, 1916)
and ct_ext_id in (6, 7)
--and stype_id = 1
group by 9 + 24 + 18, trpl_name, 'Входящие смс'
union
select 9 + 24 + 19 num, trpl_name, 'Исходящие смс внутрисеть' type, sum(duration) col
from traffic
where s_ext_id = 2
and ct_ext_id = 2
--and stype_id = 1
group by 9 + 24 + 19, trpl_name, 'Исходящие смс внутрисеть'
union
select 9 + 24 + 20 num, trpl_name, 'Исходящие смс другие операторы' type, sum(duration) col
from traffic
where s_ext_id = 2
and ct_ext_id not in (2, 5, 6, 7)
--and stype_id = 1
group by 9 + 24 + 20, trpl_name, 'Исходящие смс другие операторы'
union
select 9 + 24 + 21 num, trpl_name, 'Исходящие смс м/н' type, sum(duration) col
from traffic
where s_ext_id = 2
and ct_ext_id = 5
--and stype_id = 1
group by 9 + 24 + 21, trpl_name, 'Исходящие смс м/н'
union
select 9 + 24 + 22 num, trpl_name, 'Исходящие смс в нацроуминге внутрисеть' type, sum(duration) col
from traffic
where s_ext_id in (1409, 1288, 1916)
and ct_ext_id = 2
--and stype_id = 1
group by 9 + 24 + 22, trpl_name, 'Исходящие смс в нацроуминге внутрисеть'
union
select 9 + 24 + 23 num, trpl_name, 'Исходящие смс в нацроуминге другие операторы' type, sum(duration) col
from traffic
where s_ext_id in (1409, 1288, 1916)
and ct_ext_id not in (2, 5, 6, 7)
--and stype_id = 1
group by 9 + 24 + 23, trpl_name, 'Исходящие смс в нацроуминге другие операторы'
union
select 9 + 24 + 24 num, trpl_name, 'Исходящие смс в нацроуминге м/н' type, sum(duration) col
from traffic
where s_ext_id in (1409, 1288, 1916)
and ct_ext_id = 5
--and stype_id = 1
group by 9 + 24 + 24, trpl_name, 'Исходящие смс в нацроуминге м/н'
)
order by 1, 2
