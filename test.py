
import time
from datetime import date, date,timedelta


print(int(time.mktime(time.strptime((date.today()- timedelta(days=3+1)).strftime("%Y-%m-%d"), "%Y-%m-%d"))))
