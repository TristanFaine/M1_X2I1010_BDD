import pandas as pd
import numpy as np
import psycopg2
import uuid
from datetime import datetime

airports = pd.read_csv('airports.csv')
print(airports[(airports['iso_country'].isin(["US","PR"]))])

airports = pd.read_csv('airports.csv')

airports = airports[(airports['iso_country'].isin(["US","PR","MX","GU", "AS", "VI", "MP"]))]

airports[['longitude', 'latitude']] = airports['coordinates'].str.split(', ', 1, expand=True)

airports[['COUNTRY', 'STATE']] = airports['iso_region'].str.split('-', 1, expand=True)

for index, row in airports.iterrows():
    if(row["ident"] == "KPFN"):
        airports.loc[index,'iata_code'] = "PFN"
    if (row["iata_code"] == False) and (len(str(row['local_code'])) == 3) and (row["type"] == "closed" or row["type"] == "small_airport" or row["type"] == "medium_airport" or row["type"] == "large_airport"):
        airports.loc[index,'iata_code'] = airports.loc[index,'local_code']

airports = airports.drop(['ident','continent', 'local_code', 'gps_code', 'coordinates',  'iso_region', 'iso_country'], axis=1)

airports = airports[(airports['iata_code'].isna() == False)]

airports['elevation_ft'] = airports['elevation_ft'].fillna(0)
airports['municipality'] = airports['municipality'].fillna("Unknown")


airports.to_csv("airports_clean.csv", index=False)