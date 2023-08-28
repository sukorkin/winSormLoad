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


def send_mail_with_excel(sender_email, recipient_email, subject, path, excel_file):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipient_email)
    msg['Subject'] = subject
    # body = message
    # msg.attach(MIMEText(body, 'plain'))
    # rep = MIMEBase('application', 'octet-stream')
    fp = open(path + excel_file, 'rb')
    rep = MIMEBase('application', 'vnd.ms-excel')
    rep.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(rep)
    rep.add_header('Content-Disposition', 'attachment', filename=excel_file)
    msg.attach(rep)
    s = smtplib.SMTP('mail.ic-group.ru', 25)
    s.starttls()
    text = msg.as_string()
    s.sendmail(fromaddr, toaddr, text)


fromaddr = "Sergey.Korkin@ic-group.ru"
# toaddr = ['Sergey.Korkin@ic-group.ru']
# toaddr = ['Sergey.Korkin@ic-group.ru']
toaddr = ['Sergey.Korkin@ic-group.ru', 'Larisa.Zagorulko@ic-group.ru']
# toaddr = ['Sergey.Korkin@ic-group.ru', 'Veranika.Tyan@ic-group.ru']
# toaddr = ['Sergey.Korkin@ic-group.ru', 'Veranika.Tyan@ic-group.ru',
#           'Aleksandr.Egorov@ic-group.ru', 'Arsen.Nabiev@ic-group.ru',
#           'Ludmila.Tuchkova@ic-group.ru', 'Ekaterina.Burova@ic-group.ru']
sub = f'Новый отчет за Июль 2023'
send_mail_with_excel(fromaddr, toaddr, sub, 'reports/', 'newReport_20230701_1306.xlsx')
