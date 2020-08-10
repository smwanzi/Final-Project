# Import Dependencies
import json
import requests 
import pandas as pd
from pandas import json_normalize
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime, date

# Make API call, fetch and normalize into a dataframe
r = requests.get('https://gbfs.citibikenyc.com/gbfs/en/station_status.json')
df = json_normalize(r.json()['data']['stations'])

# Rename Columns
df2 = df.rename(columns={"num_bikes_available":"bikes_available","num_bikes_disabled":"bikes_disabled","num_docks_available":"docks_available","num_docks_disabled":"docks_disabled","last_reported":"time_reported"})

# Filtered dataframe
bike_df = df2[["station_id","bikes_available","bikes_disabled","docks_available","docks_disabled","time_reported"]]

# Function to convert epoch time to YYYY-MM-DD HH:MM:SS 
def epoch_to_readable(xepoch):
    
    return datetime.fromtimestamp(xepoch)

bike_df['time_reported'] = bike_df.apply(lambda x: epoch_to_readable(x['time_reported']), axis = 1 )

# Postgres database setup
db_connection_string = "postgres:jaigurudev@localhost:5432/FinalProject"
engine = create_engine(f'postgresql://{db_connection_string}')

# Write data to Postgres db
bike_df.to_sql(name = 'bikes',con = engine , if_exists = 'append',index = False)