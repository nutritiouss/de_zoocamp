import pandas as pd
from time import time
from sqlalchemy import create_engine

engine = create_engine('postgresql://admin:admin@localhost:5434/ny_taxi')
# !wget https: // github.com / DataTalksClub / nyc - tlc - data / releases / download / green / green_tripdata_2019 - 01.
# csv.gz
df = pd.read_csv('green_tripdata_2019-01.csv.gz', iterator=True, chunksize=10000)

while True:
    time_start = time()
    _df = next(df)
    _df.lpep_pickup_datetime = pd.to_datetime(_df.lpep_pickup_datetime)
    _df.lpep_dropoff_datetime = pd.to_datetime(_df.lpep_dropoff_datetime)
    _df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
    time_end = time()
    print(f'inserted batch {time_start - time_end} seconds')

# !wget'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'

df_zones = pd.read_csv(r'taxi+_zone_lookup.csv')
df_zones.to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')

