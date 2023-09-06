with traffic as (
select /*parallel (8)*/
trunc(time_id, 'MM') time_id,
--tp.trpl_name,
s.ext_id s_ext_id,
s.serv_name,
--ct.ext_id ct_ext_id,
--s.stype_id,
--c.type traf_type,
c.dim_subs_id,
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
--join ktk_dwh.dim_subscriber sc on c.dim_subs_id = sc.dim_subs_id
--join ktk_dwh.dim_tariff_plan tp on c.dim_trpl_id = tp.dim_trpl_id --and tp.trpl_desc = 'B2C'
--join ktk_dwh.dim_clch_type ct on c.dim_clch_id = ct.dim_clch_id
where 1 = 1
--and sc.client_type_name in ('Абонент B2C', 'Партнер', 'VIP', 'Абонент B2C ( fix )')
--and s.stype_id = 1
--and c.time_id = to_date('01.08.2023', 'DD.MM.YYYY')
--and c.time_id between to_date('01.01.2022', 'DD.MM.YYYY') and last_day(to_date('01.01.2022') + 84599/84600)
and c.time_id between to_date('01.01.2022', 'DD.MM.YYYY') and trunc(sysdate, 'MM') - 1/84600
--and tp.ext_id = 101--in (101, 58, 46, 75)
and s.ext_id in (1406, 1407, 1408, 1409, 1410, 1284, 1285, 1286, 1287, 1288, 1912, 1913, 1914, 1915, 1916)
--and c.type = 2
--and ct.ext_id in (6, 7)
--and c.dim_subs_id = 2555809261
group by s.ext_id, s.serv_name, c.dim_subs_id, trunc(time_id, 'MM')
)
select * from (
--select time_id, serv_name, 1 num, ' ' type, null col
--from traffic
--where s_ext_id = 1406
--union
select time_id, serv_name, 1 num, 'Кол-во абонентов' type, count(dim_subs_id) col
from traffic
where s_ext_id in (1406, 1284, 1912)
group by time_id, serv_name, 1, 'Кол-во абонентов'
union
select time_id, serv_name, 2 num, 'Кол-во абонентов без трафика' type, count(dim_subs_id) col --count(case when dtraf = 0 then dim_subs_id else null end) col
from traffic t
where s_ext_id in (1406, 1284, 1912)
and not exists (
select null from traffic a
where a.dim_subs_id = t.dim_subs_id and a.time_id = t.time_id
and a.s_ext_id in (1407, 1408, 1409, 1410, 1285, 1286, 1287, 1288, 1913, 1914, 1915, 1916)
)
group by time_id, serv_name, 2, 'Кол-во абонентов без трафика'
union
select time_id, serv_name, 3 num, 'Доход от абонентов без трафика' type, sum(wovat_$) col --count(case when dtraf = 0 then dim_subs_id else null end) col
from traffic t
where s_ext_id in (1406, 1284, 1912)
and not exists (
select null from traffic a
where a.dim_subs_id = t.dim_subs_id and a.time_id = t.time_id
and a.s_ext_id in (1407, 1408, 1409, 1410, 1285, 1286, 1287, 1288, 1913, 1914, 1915, 1916)
)
group by time_id, serv_name, 3, 'Доход от абонентов без трафика'
union
select time_id, substr(serv_name, 6, (length(serv_name) - 6)) serv_name, 4 num, serv_name type, sum(wovat_$) col
from traffic
where s_ext_id in (1409, 1288, 1916)
group by time_id, serv_name, 4, serv_name
union
select time_id, substr(serv_name, 21, (length(serv_name) - 21)) serv_name, 5 num, serv_name type, sum(wovat_$) col
from traffic
where s_ext_id in (1410, 1285, 1913)
group by time_id, serv_name, 5, serv_name
union
select time_id, serv_name, 6 num, 'Моя Страна  (абонплата всего)' type, sum(wovat_$) col
from traffic
where s_ext_id in (1406, 1284, 1912)
group by time_id, serv_name, 6, 'Моя Страна  (абонплата всего)'
union
select time_id, substr(serv_name, 17, (length(serv_name) - 17)), 7 num, serv_name type, sum(wovat_$) col
from traffic
where s_ext_id in (1407, 1287, 1915)
group by time_id, serv_name, 7, serv_name
union
select time_id, 'Моя Страна' serv_name, 8 num, 'Итого начислений' type, sum(wovat_$) col
from traffic
where s_ext_id in (1406, 1407, 1408, 1409, 1410)
group by time_id, 'Моя Страна', 8, 'Итого начислений'
union
select time_id, 'Поездки по России' serv_name, 8 num, 'Итого начислений' type, sum(wovat_$) col
from traffic
where s_ext_id in (1284, 1285, 1286, 1287, 1288)
group by time_id, 'Поездки по России', 8, 'Итого начислений'
union
select time_id, 'Поездки по России 2023' serv_name, 8 num, 'Итого начислений' type, sum(wovat_$) col
from traffic
where s_ext_id in (1912, 1913, 1914, 1915, 1916)
group by time_id, 'Поездки по России 2023', 8, 'Итого начислений'
)
order by 1, 2, 3