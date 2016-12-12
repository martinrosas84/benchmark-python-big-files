import pandas as pd
from sqlalchemy.engine import create_engine
import time
from multiprocessing.dummy import Pool as ThreadPool

time_start = time.time()
print("Start process")
df = pd.read_csv("2008.csv", chunksize=8000)
engine = create_engine('mysql+pymysql://root@localhost/flights')

def df_process(df):
    df = df[['Year', 'Month', 'DayofMonth', 'UniqueCarrier',
             'Origin', 'Dest', 'ArrDelay', 'FlightNum', 'TailNum']].loc[df['ArrDelay'] > 0]
    df['flight_date'] = df['Year'].astype(str) + '-' + df['Month'].astype(str) + '-' + df['DayofMonth'].astype(str)
    df['flight_date'] = pd.to_datetime(df['flight_date'])
    df = df.drop(['Year', 'Month', 'DayofMonth'], axis=1)
    df.columns = ['carrier', 'origin','destination', 'delay','flight_number','tail_number', 'flight_date']
    df['flight_number'] = df['flight_number'].astype(str)
    df.to_sql('flight_delays', engine, if_exists='append', index=False)

def putintodb(df):
    print("Start put data into db")
    time_db_start = time.time()
    engine = create_engine('mysql+pymysql://root@localhost/flights')
    df.to_sql('flight_delays', engine, if_exists='append', index=False)
    time_db_end = time.time()
    final_db_time = time_db_end - time_db_start
    print("End saving data into db. Time : ", final_db_time)

#pool = ThreadPool(30)
#pool.map(df_process, df)
#pool.close()
#pool.join()

#for chunk in df:
#    df_process(chunk)

pool = ThreadPool(30)
for chunk in df:
    pool.apply_async(df_process, [chunk])

#pool.close()
#pool.join()

time_end = time.time()
finalTime = time_end - time_start
print("End csv processing. Total time: ", finalTime)