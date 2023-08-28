import oracledb
import pandas as pd
import xlsxwriter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
import smtplib
from connector import Connector

# class Connector():
#     '''создаем класс для работы с БД'''
#     def __init__(self, user='Kirill_Tupikov', password='CrimeaRules', host='10.54.193.60', port='1521', service_name='wmdb_stb_reports'):
#         '''use your name,pass,host,port and service name to connect db'''
#         self.user = user
#         self.password = password
#         self.host = host
#         self.port = port
#         self.service_name = service_name
#
#     def create_connection(self):
#         '''this method create connection using init parametrs'''
#         dsn_tns = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
#         self.connection = oracledb.connect(user=self.user, password=self.password, dsn=dsn_tns)
#         self.cursor = self.connection.cursor()
#         print('-- Connection successful! --')
#
#     def get_query(self, query):
#         '''this method return query from db in format DataFrame'''
#         info = self.cursor.execute(query)
#         data = info.fetchall()
#         columns = [column[0] for column in info.description]
#         df = pd.DataFrame(data, columns=columns)
#         print('-- Data received successfully! --')
#         return df
    

# create object for using DB
conn = Connector()

# create connection to DB
conn.create_connection()


def get_data():
    query = '''
        select count(1) as c, d.dlr_name
        from usi_history uh
        join dealer d on d.dlr_id = uh.dlr_id
        join usi_add ua on uh.usi_id = ua.usi_id
        where ua.usi_fld_id = 14 and ua.value is not null
        and uh.usi_status_id = 7
        and uh.stime between trunc(sysdate, 'D')-7 and trunc(sysdate,'D') - 1/86400
        group by d.dlr_name
        order by 1 desc, 2
    '''
    df = conn.get_query(query)
    return df


def create_message(df):
    values = df['C']
    names = df['DLR_NAME']
    total = f'{str(values.sum()).ljust(4, " ")}  ВСЕ' + '\n' + '\n'
    message = 'Итог  Диллер' + '\n' + total
    for i in range(len(df['C'])):
        row = f'{str(values[i]).ljust(4, " ")}  {names[i]}'
        message = message + row + '\n'

    return message


def create_xlsx(df):
    writer = pd.ExcelWriter('eSim_report.xlsx', engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Лист1', index=False)
    worksheet = writer.sheets['Лист1']
    cell_range = xlsxwriter.utility.xl_range(0, 0, len(df.index), len(df.columns)-1)
    header = [{'header': 'Итог'}, {'header': 'Диллер'}]
    worksheet.add_table(cell_range, {'header_row': True, 'columns': header})
    worksheet.autofit()
    writer.close()


def send_report(message, *, xlsx=False):
    today = datetime.now()
    weekday = today.weekday()
    start_date = today - timedelta(days=weekday + 7)
    end_date = start_date + timedelta(days=6)
    fromaddr = "Operreport_daily@mobile-win.ru"
    # toaddr = ['Sergey.Korkin@ic-group.ru']
    toaddr = ['Sergey.Korkin@ic-group.ru', 'Veranika.Tyan@ic-group.ru']
    # toaddr = ['Sergey.Korkin@ic-group.ru', 'Veranika.Tyan@ic-group.ru',
    #           'Aleksandr.Egorov@ic-group.ru', 'Arsen.Nabiev@ic-group.ru',
    #           'Ludmila.Tuchkova@ic-group.ru', 'Ekaterina.Burova@ic-group.ru']
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ", ".join(toaddr)
    msg['Subject'] = f'Отчет по подключениям eSIM ВИН мобайл c {start_date.strftime("%d.%m.%Y")} по {end_date.strftime("%d.%m.%Y")}'
    body = message
    msg.attach(MIMEText(body, 'plain'))
    # rep = MIMEBase('application', 'octet-stream')
    fp = open("reports/Esim.xlsx", 'rb')
    rep = MIMEBase('application', 'vnd.ms-excel')
    rep.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(rep)
    rep.add_header('Content-Disposition', 'attachment; filename="Esim.xlsx"')
    msg.attach(rep)
    s = smtplib.SMTP('mail.ic-group.ru', 25)
    s.starttls()
    text = msg.as_string()
    s.sendmail(fromaddr, toaddr, text)


df = get_data()
message = create_message(df)
send_report(message)
