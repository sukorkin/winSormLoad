# n = int(input())
# count = 0
# countRepeat = 0
# maxCountRepeat = 0
# while count < n:
#     el = input()
#     count += 1
#     if el == '1':
#         countRepeat += 1
#     else:
#         countRepeat = 0
#     maxCountRepeat = countRepeat if countRepeat > maxCountRepeat else maxCountRepeat
#
# print(maxCountRepeat)

# import schedule
# import time
# def job():
#     print("I'm working...")
# schedule.every(10).minutes.do(job)
# schedule.every().hour.do(job)
# schedule.every().day.at("10:30").do(job)
# while 1:
#     schedule.run_pending()
#     time.sleep(1)

import pycron
import time

while True:
    if pycron.is_now('*/2 * * * *'): # is_now('0 2 * * 0'):   # True Every Sunday at 02:00
        print('running backup')
        time.sleep(60)               # The process should take at least 60 sec
                                     # to avoid running twice in one minute
    else:
        print('running sleep 15')
        time.sleep(15)               # Check again in 15 seconds