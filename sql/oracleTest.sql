/*TestDate*/
select pd.pt_id, pt.pt_def, sum(pd.wovat_$)
from pay_doc pd
inner join pay_type pt on pd.pt_id = pt.pt_id
where pd.pdoc_date between to_date('01.04.2023', 'DD.MM.YYYY') and to_date('30.04.2023', 'DD.MM.YYYY')
group by pd.pt_id, pt.pt_def
order by pd.pt_id