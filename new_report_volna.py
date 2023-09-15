import pandas as pd
import numpy as np
import xlsxwriter
import locale
import datetime
from connector import Connector


def start_task():
    def get_full_table(prev_month):
        fd = open('sql/newReportVolna.sql', 'r', encoding='utf-8')
        query = fd.read().replace('01.01.2020', prev_month.strftime("%d.%m.%Y"))
        fd.close()
        df = conn.get_query(query)
        return df

    def prepare_data(df):
        df = df.T.drop_duplicates().T
        # df['MDATE'] = pd.to_datetime(df['MDATE']).dt.strftime('%B %Y')
        # df['FIRST_CALL'] = pd.to_datetime(df['FIRST_CALL']).dt.strftime('%d-%m-%Y')
        # df['WD_FIRST_DATE'] = pd.to_datetime(df['WD_FIRST_DATE']).dt.strftime('%d-%m-%Y')
        # df['SIGN_DATE'] = pd.to_datetime(df['SIGN_DATE']).dt.strftime('%d-%m-%Y')
        # df['PARTY_DATE'] = pd.to_datetime(df['PARTY_DATE']).dt.strftime('%d-%m-%Y')
        # df['SYS_INC_DATE'] = pd.to_datetime(df['SYS_INC_DATE']).dt.strftime('%d-%m-%Y')
        # df['LAST_EDIT_CNTR_DATE'] = pd.to_datetime(df['LAST_EDIT_CNTR_DATE']).dt.strftime('%d-%m-%Y')
        # df['LAST_CALL'] = pd.to_datetime(df['LAST_CALL']).dt.strftime('%d-%m-%Y')
        # df['LAST_ACTIVITY_DATE'] = pd.to_datetime(df['LAST_ACTIVITY_DATE']).dt.strftime('%d-%m-%Y')
        # df['MSISDN'] = df['MSISDN'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['PARTY_NUM'] = df['PARTY_NUM'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['ACTIVATION_IMSI'] = df['ACTIVATION_IMSI'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['ACTIVATION_ICC'] = df['ACTIVATION_ICC'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['IMSI'] = df['IMSI'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['COL'] = df['COL'].apply(lambda x: np.NaN if pd.isnull(x) else float(x))
        # df['SALE_POINT'] = df['SALE_POINT'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        return df

    def get_report(df):
        report = df.pivot_table(values='COL', columns='TRPL_NAME', index=['TYPE'], sort=False,
                                aggfunc='sum', margins=True, margins_name='Итог')

        return report

    def create_worksheet(writer, df, sheet_name):
        df = df.reset_index()
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        cell_range = xlsxwriter.utility.xl_range(0, 0, len(df.index),
                                                 len(df.columns) - 1)
        header = [{'header': name} for name in df.columns]
        header[0] = {'header': ' '}
        # header.insert(0, {'header': ' '})
        worksheet.add_table(cell_range, {'header_row': True, 'columns': header})
        if sheet_name == 'Детали':
            workbook = writer.book
            format = workbook.add_format({'num_format': '0'})
            worksheet.set_column('AF:AI', None, format)
        worksheet.autofit()

    def write_xlsx_file(esim, df, prev_month):
        rep_name = 'reports/newReportVolna' + '_' + prev_month.strftime("%Y%m") + '.xlsx'
        # rep_name = 'reports/newReportVolna' + '_' + today.strftime("%Y%m%d_%H%M") + '.xlsx'
        writer = pd.ExcelWriter(rep_name, engine='xlsxwriter')
        create_worksheet(writer, esim, prev_month.strftime('%B %Y'))
        # create_worksheet(writer, df, 'Детали')
        writer.close()
        print('-- Excel file created successful! --')

    # today = datetime.date.today()
    prev_month = today.replace(day=1, month=today.month - 1)
    df = get_full_table(prev_month)
    df = prepare_data(df)
    rep = get_report(df)
    write_xlsx_file(rep, df, prev_month)


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
