import csv
import cx_Oracle


lib_dir = r"C:\app\product\instantclient_21_10"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)
# pool  = cx_Oracle.SessionPool(db_config.user, db_config.pw, db_config.dsn,min = 5, max = 6, increment = 1)
connection = cx_Oracle.connect('user', 'password', '10.54.193.60/WMDB_STB_REPORTS')

file = open(r"C:\work\test\file.reports", "w")

output = csv.writer(file, delimiter=';', lineterminator="\n")

cur = connection.cursor()

cur.arraysize=250000

cur.execute('select * from pay_type')

columns = [i[0] for i in cur.description]

output.writerow(columns)

while True:

    rows = cur.fetchmany()

    if not rows:
        break

    output.writerows(rows)

file.close()
