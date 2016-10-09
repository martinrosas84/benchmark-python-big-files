import pandas as pd
from sqlalchemy.engine import create_engine
import time

"""
Use pandas, the code needs improvements for clarity and, maybe for performance
First approach:
Python time: 50 seconds.
Data into db: 211 seconds
"""

time_start = time.time()
print("Start csv processing")
df = pd.read_csv("2008.csv")
df = df[['Year', 'Month', 'DayofMonth', 'UniqueCarrier',
         'Origin', 'Dest', 'ArrDelay', 'FlightNum', 'TailNum']].loc[df['ArrDelay'] > 0]
df['flight_date'] = df['Year'].astype(str) + '-' + df['Month'].astype(str) + '-' + df['DayofMonth'].astype(str)
df['flight_date'] = pd.to_datetime(df['flight_date'])
df = df.drop(['Year', 'Month', 'DayofMonth'], axis=1)
df.columns = ['carrier', 'origin','destination', 'delay','flight_number','tail_number', 'flight_date']
df['flight_number'] = df['flight_number'].astype(str)
time_end = time.time()
finalTime = time_end - time_start
print("End csv processing. Time when proccess python: ", finalTime)
print("Start put data into db")
time_db_start = time.time()
engine = create_engine('mysql+pymysql://root@localhost/flights')
df.to_sql('flight_delays', engine, if_exists='append', index=False)
time_db_end = time.time()
final_db_time = time_db_end - time_db_start
print("End saving data into db. Time : ", final_db_time)
