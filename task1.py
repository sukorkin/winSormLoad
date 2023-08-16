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
        '''данная функция возвращает все данные из таблицы KTK_DWH.DIM_SUBSCRIBER за текущий месяц'''
        query = '''
            select *
            from ktk_dwh.dim_subscriber left join reports.dlr_desc using(DLR_ID)
            where trunc(activation_date, 'DD') between trunc(sysdate, 'mm') and trunc(sysdate-1, 'DD')
            order by activation_date
            '''
        df = conn.get_query(query)
        return df

    def prepare_data(df):
        df = df.T.drop_duplicates().T
        df['ACTIVATION_DATE'] = pd.to_datetime(df['ACTIVATION_DATE']).dt.strftime('%d-%m-%Y')
        df['FIRST_CALL'] = pd.to_datetime(df['FIRST_CALL']).dt.strftime('%d-%m-%Y')
        df['WD_FIRST_DATE'] = pd.to_datetime(df['WD_FIRST_DATE']).dt.strftime('%d-%m-%Y')
        df['SIGN_DATE'] = pd.to_datetime(df['SIGN_DATE']).dt.strftime('%d-%m-%Y')
        df['PARTY_DATE'] = pd.to_datetime(df['PARTY_DATE']).dt.strftime('%d-%m-%Y')
        df['SYS_INC_DATE'] = pd.to_datetime(df['SYS_INC_DATE']).dt.strftime('%d-%m-%Y')
        df['LAST_EDIT_CNTR_DATE'] = pd.to_datetime(df['LAST_EDIT_CNTR_DATE']).dt.strftime('%d-%m-%Y')
        df['LAST_CALL'] = pd.to_datetime(df['LAST_CALL']).dt.strftime('%d-%m-%Y')
        df['LAST_ACTIVITY_DATE'] = pd.to_datetime(df['LAST_ACTIVITY_DATE']).dt.strftime('%d-%m-%Y')
        df['MSISDN'] = df['MSISDN'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['PARTY_NUM'] = df['PARTY_NUM'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['ACTIVATION_IMSI'] = df['ACTIVATION_IMSI'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['ACTIVATION_ICC'] = df['ACTIVATION_ICC'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['IMSI'] = df['IMSI'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['ICC'] = df['ICC'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        df['SALE_POINT'] = df['SALE_POINT'].apply(lambda x: np.NaN if pd.isnull(x) else int(x))
        return df

    def get_nomenclature(df):
        '''создаем сводную таблицу по листу (Номенклатура)'''
        nomenclature = df.pivot_table(values='ICC', columns='ACTIVATION_DATE', index=['CHANNEL_NAME', 'DLR_NAME'],
                                      aggfunc='count', margins=True, margins_name='Итог')
        return nomenclature

    def get_tp_activation(df):
        '''создаем сводную таблицу по листу (ТП на момент активации)'''
        tp_activation = df.pivot_table(values='ICC', columns='ACTIVATION_DATE', index=['DLR_NAME', 'FIRST_TRPL_NAME'],
                                       aggfunc='count', margins=True, margins_name='Итог')
        return tp_activation

    def get_tp_split(df):
        '''создаем сводную таблицу по листу (Сплит по ТП)'''
        tp_split = df.pivot_table(values='ICC', columns='ACTIVATION_DATE', index=['CHANNEL_NAME', 'TRPL_NAME'],
                                  aggfunc='count', margins=True, margins_name='Итог')
        return tp_split

    def create_worksheet(writer, df, sheet_name):
        df = df.reset_index()
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]  # получаем лист с которым работаем
        cell_range = xlsxwriter.utility.xl_range(0, 0, len(df.index),
                                                 len(df.columns) - 1)  # создаем диапазон для таблицы
        header = [{'header': name} for name in df.columns]  # генерируем заголовки для таблицы
        worksheet.add_table(cell_range, {'header_row': True,
                                         'columns': header})  # создаем таблицу в выбраном диапазоне с нужными заголовками
        if sheet_name == 'Детали':
            workbook = writer.book
            format = workbook.add_format({'num_format': '0'})
            worksheet.set_column('AF:AI', None, format)
        worksheet.autofit()  # устанавливаем авторазмер для столбцов листа

    def write_xlsx_file(nomenclature, tp_activation, tp_split, df):
        writer = pd.ExcelWriter('reports/Example.xlsx', engine='xlsxwriter')  # !!! тут можно изменить название файла
        create_worksheet(writer, nomenclature, 'Номенклатура')
        create_worksheet(writer, tp_activation, 'ТП на момент активации')
        create_worksheet(writer, tp_split, 'Сплит по ТП')
        # create_worksheet(writer, df, 'Детали')
        writer.close()
        print('-- Excel file created successful! --')

    df = get_full_table()  # получили данные
    df = prepare_data(df)
    nomenclature = get_nomenclature(df)  # построили таблицу для листа Номенклатура
    tp_activation = get_tp_activation(df)  # построили таблицу для листа ТП на момент активации
    tp_split = get_tp_split(df)  # построили таблицу для листа Сплит по ТП
    write_xlsx_file(nomenclature, tp_activation, tp_split, df)  # формируем Excel файл


start_task()
