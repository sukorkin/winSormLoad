import pandas as pd
import numpy as np
import xlsxwriter
import locale
import datetime
from connector import Connector


def start_task():
    def get_full_table():
        query = '''
                select dc.client_name "Имя участника программы", dc.client_type_name "Тип клиента", dc.inn "ИНН участника программы",
                        dc.id_series "Серия паспорта", dc.id_number "Номер паспорта", pbp.sender "Номер участника программы",
                        pbp.recipient "Сервисный номер", pbp.receipt_date "Дата регистрации на номер 777", pbp.message_text "Текст сообщения",
                        ds1.wd_first_date "Дата и время регистрации в WD", ds1.activation_date "Дата и время активации СП", ds1.trpl_name "Тарифный план",
                        pbpa.bonus "Бонус", ds1.sale_point "Точка продаж", ds1.dlr_name "Наименование Дилера"
                from ktk.partner_bonus_program@ktkdb2 pbp
                inner join ktk_dwh.dim_subscriber ds1 on ds1.msisdn = pbp.message_text
                inner join ktk_dwh.dim_subscriber ds2 on ds2.msisdn = pbp.sender
                inner join ktk_dwh.dim_client dc on ds2.clnt_id = dc.ext_id
                left join ktk.partner_bonus_program_ap_log@ktkdb2 pbpa on pbp.sender =  pbpa.sender and receipt_date < date_m
                where trunc(receipt_date) between trunc(sysdate, 'mm')and trunc(sysdate, 'DD') - 1/86400
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
conn = Connector()
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
