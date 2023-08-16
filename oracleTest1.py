import csv
import datetime
import config
from connector import Connector

today = datetime.datetime.today()
print('Start: ' + today.strftime("%d.%m.%Y %H:%M:%S"))

conn = Connector()
conn.create_connection()

# dsn = f'{config.username}/{config.password}@{config.host}:{config.port}/{config.service_name}'
# connection = oracledb.connect(dsn)
# # connection = oracledb.connect(user=config.username, password=config.password, dsn=config.dsn_win)
# cur = connection.cursor()
# cur.arraysize = config.arraysize

fd = open('sql/oracleTest.sql', 'r')
sqlNameLine = fd.readline()
sqlFile = fd.read()
fd.close()

fileName = sqlNameLine.replace('/', '').replace('*', '').replace('\n', '').replace('\r', '')
fileName = "reports/" + fileName + '_' + today.strftime("%Y%m%d_%H%M") + '.txt'
# file = open(r"C:\Users\sergey.korkin\Documents\Export\python\file.reports", "w")
file = open(fileName, "w", newline="", encoding=config.encoding)
output = csv.writer(file, delimiter=config.delimiter, lineterminator=config.lineterminator)

# cur.execute(sqlFile)
cur = conn.get_cursor(sqlFile)
print('Load file: ' + 'sql/oracleTest1.sql')
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
