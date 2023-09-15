import pandas as pd
import numpy as np
import xlsxwriter
import locale
import datetime
from connector import Connector


def start_task():
    def get_full_table():
        query = '''
                with bonus as(
                select par.sender, par.message_text, 
                      case when s.trpl_id=103  then 50 else 0 end as bonus
                from (
                    select *
                    from ktk.partner_bonus_program@ktkdb2
                    where receipt_date>=add_months(trunc(sysdate,'mm'),-1)) par
                inner join phone@ktkdb2 p                 on par.message_text=p.msisdn
                inner join subs_history@ktkdb2 s          on p.phone_id=s.phone_id  and s.trpl_id in (103,107,102,85) and s.num_history=1 ---- по первой версии найдем номер
                inner join subscriber@ktkdb2 su           on su.SUBS_ID=s.SUBS_ID   and trunc(su.ACTIVATION_DATE,'mm')= add_months(trunc(sysdate,'mm'),-1)  and ABS(par.RECEIPT_DATE-su.ACTIVATION_DATE)*24 <= 72 
                inner join subs_history@ktkdb2 s1         on s.subs_id=s1.subs_id   and s1.etime>sysdate     and s1.stat_id!=3   ---- по последней поймем что абонент еще не закрыт
                inner join subs_opt_data@ktkdb2 o         on s.subs_id=o.subs_id    and o.etime>sysdate     and o.subs_fld_id in (67,12) and o.field_value not in (select code_tt from ktk.partner_tt_not_use@ktkdb2 where status=1 and etime>sysdate)
                inner join contract@ktkdb2 c1             on c1.CLNT_ID = s.CLNT_ID and c1.etime>sysdate     and trunc(c1.SIGN_DATE,'mm')= add_months(trunc(sysdate,'mm'),-1)  and ABS(c1.SIGN_DATE-su.ACTIVATION_DATE)*24 <= 72  --Сведения об абоненте внесены в WIC в срок не позднее 72 часов с момента активации СП
                )
                select dc.client_name "Имя участника программы", dc.client_type_name "Тип клиента", dc.inn "ИНН участника программы",
                        dc.id_series "Серия паспорта", dc.id_number "Номер паспорта", pbp.sender "Номер участника программы",
                        pbp.recipient "Сервисный номер", pbp.receipt_date "Дата регистрации на номер 777", pbp.message_text "Текст сообщения",
                        ds1.wd_first_date "Дата и время регистрации в WD", ds1.activation_date "Дата и время активации СП", ds1.SIGN_DATE "Дата подписи",
                        ds1.invcode_name "Номенклатура", ds1.trpl_name "Тарифный план", nvl(pbpa.bonus, 0) "Бонус",
                        ds1.sale_point "Точка продаж", ds1.dlr_name "Наименование Дилера"
                from ktk.partner_bonus_program@ktkdb2 pbp
                inner join ktk_dwh.dim_subscriber ds1 on ds1.msisdn = pbp.message_text
                inner join ktk_dwh.dim_subscriber ds2 on ds2.msisdn = pbp.sender
                inner join ktk_dwh.dim_client dc on ds2.clnt_id = dc.ext_id
                left join bonus pbpa on pbp.sender = pbpa.sender and pbp.message_text = pbpa.message_text --and date_m = add_months(trunc(sysdate,'mm'),-1)
                where trunc(receipt_date) between add_months(trunc(sysdate,'mm'),-1) and last_day(add_months(trunc(sysdate,'mm'),-1)) + 86399/86400
                '''
        df = conn.get_query(query)
        return df

    def prepare_data(df):
        df = df.T.drop_duplicates().T
        # df['STIME'] = pd.to_datetime(df['STIME']).dt.strftime('%d-%m-%Y')
        # df['FIRST_CALL'] = pd.to_datetime(df['FIRST_CALL']).dt.strftime('%d-%m-%Y')
        # df['WD_FIRST_DATE'] = pd.to_datetime(df['WD_FIRST_DATE']).dt.strftime('%d-%m-%Y')
        # df['SIGN_DATE'] = pd.to_datetime(df['SIGN_DATE']).dt.strftime('%d-%m-%Y')
        # df['PARTY_DATE'] = pd.to_datetime(df['PARTY_DATE']).dt.strftime('%d-%m-%Y')
        # df['SYS_INC_DATE'] = pd.to_datetime(df['SYS_INC_DATE']).dt.strftime('%d-%m-%Y')
        # df['LAST_EDIT_CNTR_DATE'] = pd.to_datetime(df['LAST_EDIT_CNTR_DATE']).dt.strftime('%d-%m-%Y')
        # df['LAST_CALL'] = pd.to_datetime(df['LAST_CALL']).dt.strftime('%d-%m-%Y')
        df['Дата и время активации СП'] = pd.to_datetime(df['Дата и время активации СП']).dt.strftime('%d.%m.%Y %H:%M:%S')
        df['Дата регистрации на номер 777'] = pd.to_datetime(df['Дата регистрации на номер 777']).dt.strftime('%d.%m.%Y %H:%M:%S')
        df['Дата и время регистрации в WD'] = pd.to_datetime(df['Дата и время регистрации в WD']).dt.strftime('%d.%m.%Y %H:%M:%S')
        # df['MSISDN'] = df['MSISDN'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['PARTY_NUM'] = df['PARTY_NUM'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['ACTIVATION_IMSI'] = df['ACTIVATION_IMSI'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['ACTIVATION_ICC'] = df['ACTIVATION_ICC'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['IMSI'] = df['IMSI'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['CNT'] = df['CNT'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['SALE_POINT'] = df['SALE_POINT'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        return df

    # def get_partners(df):
    #     '''создаем сводную таблицу по листу (Номенклатура)'''
    #     esim = df.pivot_table(values='CNT', columns='STIME', index=['DLR_NAME'],
    #                           aggfunc='count', fill_value=0, margins=True, margins_name='Итог')
    #     return esim

    def create_worksheet(writer, df, sheet_name):
        # df = df.reset_index()
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        cell_range = xlsxwriter.utility.xl_range(0, 0, len(df.index),
                                                 len(df.columns) - 1)
        header = [{'header': name} for name in df.columns]
        worksheet.add_table(cell_range, {'header_row': True,
                                         'columns': header})
        if sheet_name == 'Детали':
            workbook = writer.book
            format = workbook.add_format({'num_format': '0'})
            worksheet.set_column('AF:AI', None, format)
        worksheet.autofit()

    def write_xlsx_file(partners, df):
        writer = pd.ExcelWriter('reports/Partners.xlsx', engine='xlsxwriter')
        create_worksheet(writer, partners, 'Partners')
        # create_worksheet(writer, df, 'Детали')
        writer.close()
        print('-- Excel file created successful! --')

    df = get_full_table()
    df = prepare_data(df)
    # partners = get_partners(df)
    write_xlsx_file(df, df)


today = datetime.datetime.today()
print('Start: ' + today.strftime("%d.%m.%Y %H:%M:%S"))
# create object for using DB
conn = Connector(False)
# create connection to DB
conn.create_connection()

locale.setlocale(locale.LC_ALL, ('ru_RU', 'UTF-8'))
# locale.setlocale(locale.LC_TIME, 'ru')
# locale.setlocale(category=locale.LC_ALL, locale="Russian")
# locale.setlocale(category=locale.LC_ALL, locale="")
start_task()
today2 = datetime.datetime.today()
print('Execution time: ' + str(today2 - today))
print('End: ' + today2.strftime("%d.%m.%Y %H:%M:%S"))
