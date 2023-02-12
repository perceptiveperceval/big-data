
import time
from datetime import date,timedelta, datetime


path = "simulation/current_time.txt"

with open(path, "r") as f:
    line = f.readlines()[0]  # Y, m, d
    start_date = datetime.strptime(line, "%Y-%m-%d")
    end_date = start_date + timedelta(days=7)

fd = int(time.mktime(start_date.timetuple()))
td = int(time.mktime(end_date.timetuple()))

with open(path, "w") as f:
    f.write(end_date.strftime("%Y-%m-%d"))
