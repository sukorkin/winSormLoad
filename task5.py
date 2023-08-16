import datetime
import glob
import config


def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 60)
    return days, hours, minutes, seconds


def addDigits(num):
    # x = [int(a) for a in str(num)]
    s = sum(int(numb) for numb in str(num))
    # s = 0
    # for i in x:
    #     s += i
    if s > 9:
        s = addDigits(s)
    return s


addDigits1 = lambda x: sum(int(numb) for numb in str(x))

print(addDigits(597))
print(addDigits1(597))
print(config.dsn)
print(config.port)
print(config.encoding)
print(config.username)
print(config.password)
# print(int(sorted(input()) == sorted(input())))
print(datetime.date(2023, 4, 1))
print(datetime.date.today())
today = datetime.datetime.today()
print(today.strftime("%Y%m%d_%H%M"))
delta = today - (datetime.datetime(2023, 8, 1))
print(delta)
days, hours, minutes, seconds = convert_timedelta(delta)
print('Load time: ' + str(delta.seconds) + 'c')
print('Load time: ' + str(delta))
print('Load time: {0} Day {1} Hour {2} Minute {3} Second'.format(days, hours, minutes, seconds))
print(glob.glob("sql/*.sql"))
print('---------------------')
# for fileName in glob.glob("sql/*.sql"):
#     fd = open(fileName, 'r')
#     sqlNameLine = fd.readline()
#     sqlFile = fd.read()
#     print(fd.name)
#     fd.close()
#     print(sqlNameLine)
#     print(sqlFile)
#     print('---------------------')

fd = open('resources/lastLoad.txt', 'r')
sqlNameLine = fd.readline()
fd.close()
print(sqlNameLine)
f = "%d.%m.%Y %H:%M:%S"
out = datetime.datetime.strptime(sqlNameLine, f)
print(out)
