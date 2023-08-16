import csv
import datetime
import glob
import oracledb
import config
from connector import Connector


def create_report(sql_file_name):
    print()
    today = datetime.datetime.today()
    print('Start: ' + today.strftime("%d.%m.%Y %H:%M:%S"))

    # dsn = f'{config.username}/{config.password}@{config.host}:{config.port}/{config.service_name}'
    # connection = oracledb.connect(dsn)
    # # connection = oracledb.connect(user=config.username, password=config.password, dsn=config.dsn_win)
    # cur = connection.cursor()
    # cur.arraysize = config.arraysize

    fd = open(sql_file_name, 'r')
    line = fd.readline()
    sql = fd.read()
    fd.close()

    filename = line.replace('/', '').replace('*', '').replace('\n', '').replace('\r', '')
    filename = "reports/" + filename + '_' + today.strftime("%Y%m%d_%H%M") + '.txt'
    # file = open(r"C:\Users\sergey.korkin\Documents\Export\python\file.reports", "w")
    file = open(filename, "w", newline="", encoding=config.encoding)
    output = csv.writer(file, delimiter=config.delimiter, lineterminator=config.lineterminator)

    print('Execute file: ' + sql_file_name)
    # cur.execute(sql)
    cur = conn.get_cursor(sql)
    print('Write file: ' + file.name)

    columns = [i[0] for i in cur.description]
    output.writerow(columns)

    while True:
        rows = cur.fetchmany()
        if not rows:
            break

        output.writerows(rows)

    file.close()

    today2 = datetime.datetime.today()
    print('Execution time: ' + str(today2 - today))
    print('End: ' + today2.strftime("%d.%m.%Y %H:%M:%S"))


conn = Connector()
conn.create_connection()

for fileName in glob.glob("sql/*.sql"):
    create_report(fileName)
