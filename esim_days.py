import pandas as pd
import numpy as np
import xlsxwriter
from connector import Connector


# create object for using DB
conn = Connector()

# create connection to DB
conn.create_connection()


def start_task():
    def get_full_table():
        query = '''
                select d.dlr_name, trunc(uh.stime) stime,  uh.usi_id cnt
                from usi_history uh
                join dealer d on d.dlr_id = uh.dlr_id
                join usi_add ua on uh.usi_id = ua.usi_id
                where ua.usi_fld_id = 14 and ua.value is not null --and slv.msisdn is not null
                and uh.usi_status_id = 7
                and stime between trunc(sysdate, 'D')-7 and trunc(sysdate,'D') - 1/86400
                --group by d.dlr_name, trunc(uh.stime)
                --order by 1 desc, 2
                '''
        df = conn.get_query(query)
        return df

    def prepare_data(df):
        df = df.T.drop_duplicates().T
        df['STIME'] = pd.to_datetime(df['STIME']).dt.strftime('%d-%m-%Y')
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
        df['CNT'] = df['CNT'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        # df['SALE_POINT'] = df['SALE_POINT'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        return df

    def get_esim(df):
        esim = df.pivot_table(values='CNT', columns='STIME', index=['DLR_NAME'],
                                      aggfunc='count', fill_value=0, margins=True, margins_name='Итог')
        return esim

    def create_worksheet(writer, df, sheet_name):
        df = df.reset_index()
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

    def write_xlsx_file(esim, df):
        writer = pd.ExcelWriter('reports/Esim.xlsx', engine='xlsxwriter')
        create_worksheet(writer, esim, 'Esim')
        # create_worksheet(writer, df, 'Детали')
        writer.close()
        print('-- Excel file created successful! --')

    df = get_full_table()
    df = prepare_data(df)
    esim = get_esim(df)
    write_xlsx_file(esim, df)


start_task()
