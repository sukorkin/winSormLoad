/*ABONENT*/
  select distinct ID,
         REGION_ID,
         CONTRACT_DATE,
         CONTRACT,
         ACTUAL_FROM,
         ACTUAL_TO,
         ABONENT_TYPE,
         case
           when FAMILY_NAME is null or GIVEN_NAME is null or INITIAL_NAME is null then
            '1'
           else
            '0'
         end as NAME_INFO_TYPE,
         replace(FAMILY_NAME, ';', '') as FAMILY_NAME,
         replace(GIVEN_NAME, ';', '') as GIVEN_NAME,
         replace(INITIAL_NAME, ';', '') as INITIAL_NAME,
         replace(UNSTRUCT_NAME, ';', '') as UNSTRUCT_NAME,
         BIRTH_DATE,
         IDENT_CARD_TYPE_ID,
         IDENT_CARD_TYPE,
         replace(IDENT_CARD_SERIAL, ';', '') as IDENT_CARD_SERIAL,
         IDENT_CARD_NUMBER,
         replace(replace(replace(IDENT_CARD_DESCRIPTION, CHR(10), ''), CHR(13), ''), ';', ',') as IDENT_CARD_DESCRIPTION,
         replace(replace(replace(IDENT_CARD_UNSTRUCT, CHR(10), ''), CHR(13), ''), ';', ',') as IDENT_CARD_UNSTRUCT,
         BANK,
         BANK_ACCOUNT,
         replace(FULL_NAME, ';', '') as FULL_NAME,
         INN,
         replace(CONTACT, ';', '') as CONTACT,
         replace(PHONE_FAX, ';', ',') as PHONE_FAX,
         STATUS,
         ATTACH,
         DETACH,
         NETWORK_TYPE,
         INTERNAL_ID1,
         INTERNAL_ID2
    from (select to_char(sh.subs_id) as ID,
                 to_char(ch.branch_id + 1) as REGION_ID,
                 to_char(case
                   when nvl(c.sign_date, c.stime) < to_date('01.01.1970', 'DD.MM.YYYY') then
                    '1971-01-01 23:59:59'
                   else
                    to_char(nvl(c.sign_date, c.stime), 'YYYY-MM-DD HH24:MI:SS')
                 end) as CONTRACT_DATE,
                 c.contract_num as CONTRACT,
                 to_char(c.stime, 'YYYY-MM-DD HH24:MI:SS') as ACTUAL_FROM,
                 case
                   when c.etime >= to_date('31.12.2999', 'DD.MM.YYYY') then
                    '2049-12-31 23:59:59'
                   else
                    to_char(c.etime, 'YYYY-MM-DD HH24:MI:SS')
                 end as ACTUAL_TO,
                 case ct.cust_type_id
                   when 1 then
                    '42'
                   else
                    '43'
                 end as ABONENT_TYPE,
                 --case
                   --when ch.customer_id is not null then
                    --'0'
                   --else
                    --'1'
                 --end as NAME_INFO_TYPE,
                 (select cn_value
                    from customer_name_trn cnt
                   where cnt.customer_id = ch.customer_id
                     and cnt.lang_id = 1
                     and cnt.cn_dict_code = 'surname'
                     and del_date is null) as FAMILY_NAME,
                 (select cn_value
                    from customer_name_trn cnt
                   where cnt.customer_id = ch.customer_id
                     and cnt.lang_id = 1
                     and cnt.cn_dict_code = 'name'
                     and del_date is null
                     and rownum = 1) as GIVEN_NAME,
                 (select cn_value
                    from customer_name_trn cnt
                   where cnt.customer_id = ch.customer_id
                     and cnt.lang_id = 1
                     and cnt.cn_dict_code = 'patronymic'
                     and del_date is null) as INITIAL_NAME,
                 c.name as UNSTRUCT_NAME,
                 case ct.cust_type_id
                   when 1 then
                    to_char(c.date_of_birth, 'YYYY-MM-DD HH24:MI:SS')
                   else
                    ''
                 end as BIRTH_DATE,
                 to_char(cid.iddoc_id) as IDENT_CARD_TYPE_ID,
                 case
                   when ch.customer_id is not null then
                    '0'
                   else
                    '1'
                 end as IDENT_CARD_TYPE,
                 substr(trim(cid.id_series), 1, 15) as IDENT_CARD_SERIAL,
                 substr(trim(cid.id_number), 1, 15) as IDENT_CARD_NUMBER,
                 to_char(cid.id_date_of_issue, 'YYYY-MM-DD HH24:MI:SS') || ' ' || cid.id_authority as IDENT_CARD_DESCRIPTION,
                 c.pasport as IDENT_CARD_UNSTRUCT,
                 null as BANK,
                 null as BANK_ACCOUNT,
                 substr(trim(c.name), 1, 128) as FULL_NAME,
                 c.inn as INN,
                 c.contact_face as CONTACT,
                 case
                   when c.phone is null then
                    ch.fax
                   else
                    c.phone
                 end as PHONE_FAX,
                 case
                   when sh.stat_id = 1 then
                    '0'
                   else
                    '1'
                 end as STATUS,
                  case
                      when ct.cust_type_id = 1
                      then
                          (select to_char(stime_sim, 'YYYY-MM-DD HH24:MI:SS')
                             from (select suh.*,
                                          uh.usi_status_id,
                                          uh.stime                             stime_sim,
                                          max (suh.num_history)
                                              over (
                                                  partition by suh.subs_id)    last_num_hist
                                     from subs_usi_history suh,
                                          usi_history     uh
                                    where     suh.subs_id = sh.subs_id
                                          and uh.usi_id = suh.usi_id) s
                            where     s.num_history = last_num_hist
                                  and s.usi_status_id = 7
                                  and rownum = 1)
                      else
                          to_char(nvl(c.sign_date, c.stime), 'YYYY-MM-DD HH24:MI:SS')
                  end
                      as ATTACH,
                      (select to_char(stime_sim, 'YYYY-MM-DD HH24:MI:SS')
                         from (select suh.*,
                                      uh.usi_status_id,
                                      uh.stime                               stime_sim,
                                      max (suh.num_history)
                                          over (partition by suh.subs_id)    last_num_hist
                                 from subs_usi_history suh, usi_history uh
                                where     suh.subs_id = sh.subs_id
                                      and uh.usi_id = suh.usi_id) s
                        where     s.num_history = last_num_hist
                              and s.usi_status_id = 6
                              and rownum = 1)
                          as DETACH,
                 '1' as NETWORK_TYPE,
                 to_char(sh.subs_id) as INTERNAL_ID1,
                 null as INTERNAL_ID2
            from SUBS_HISTORY               sh,
                 client_history             ch,
                 contract                   c,
                 client_jur_type            cjt,
                 customer_type              ct,
                 customer_identify_document cid
           where sh.clnt_id = ch.clnt_id
             and ch.clnt_id = c.clnt_id
             and ch.cjt_id = cjt.cjt_id
             and cjt.cust_type_id = ct.cust_type_id
             and ch.customer_id = cid.customer_id(+)
             and sh.stime <= sysdate
             and sysdate < sh.etime
             and ch.stime <= sysdate
             and sysdate < ch.etime
             and c.stime <= sysdate
             and sysdate < c.etime
             and sh.stime <= sysdate
             and cid.cre_date(+) <= sysdate
             and sysdate < nvl(cid.del_date, to_date('31.12.2999', 'DD.MM.YYYY'))
           order by c.clnt_id, c.num_history)