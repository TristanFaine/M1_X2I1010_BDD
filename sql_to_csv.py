import pandas as pd
import numpy as np
import psycopg2
import uuid
from datetime import datetime

def connect():
    return psycopg2.connect(
        host="54.38.243.66",
        port="49801",
        user="groupe",
        password="24DK1jAudkZo_32kd01mDkzNxId4214ZdPAZncJAPd291",
        database="flights-delay"
    )
    
drops = (
"""
DROP TABLE IF EXISTS FLIGHT CASCADE
""",
"""
DROP TABLE IF EXISTS AIRLINE CASCADE
""",
"""
DROP TABLE IF EXISTS DATE CASCADE
""",
"""
DROP TABLE IF EXISTS AIRPORT CASCADE
"""
)

conn = connect()
cur = conn.cursor()
for drop in drops:
    cur.execute(drop)
conn.commit()
cur.close()

creates = (
"""CREATE TABLE IF NOT EXISTS AIRLINE (
    AIRLINE_CODE character(2) NOT NULL,
    NAME text NOT NULL,
    COUNTRY text NOT NULL,
    ACTIVE boolean NOT NULL,
    
    PRIMARY KEY (AIRLINE_CODE)
)""",
"""CREATE TABLE IF NOT EXISTS DATE (
    DATE text NOT NULL,
    YEAR NUMERIC(4) NOT NULL,
    MONTH NUMERIC(2) NOT NULL,
    DAY NUMERIC(2) NOT NULL,
    SEASON NUMERIC(1) NOT NULL,
    HOURS NUMERIC(2) NOT NULL,
    MINUTES NUMERIC(2) NOT NULL,

    PRIMARY KEY (DATE)
)""",
"""CREATE TABLE IF NOT EXISTS AIRPORT (
    AIRPORT_CODE character(3) NOT NULL,
    NAME text NOT NULL,
    COUNTRY character(2) NOT NULL,
    STATE character(3) NOT NULL,
    CITY text NOT NULL,
    TYPE text NOT NULL,
    LONGITUDE float NOT NULL,
    LATITUDE float NOT NULL,
    ELEVATION integer NOT NULL,
    
    PRIMARY KEY (AIRPORT_CODE)
)""",
"""CREATE TABLE IF NOT EXISTS FLIGHT (
    FLIGHT_ID uuid NOT NULL,
    DATE text NOT NULL,
    AIRLINE_CODE character(2) NOT NULL,
    ARRIVAL_AIRPORT character(3) NOT NULL,
    DEPARTURE_AIRPORT character(3) NOT NULL,
    DISTANCE integer NOT NULL,
    EST_DEP_HOUR smallint NOT NULL,
    REAL_DEP_HOUR smallint NOT NULL,
    DEP_DELAY smallint NOT NULL, 
    EST_ARR_HOUR smallint NOT NULL,
    ARR_HOUR smallint NOT NULL,
    ARR_DELAY smallint NOT NULL,
    EST_ELAPSED_TIME integer NOT NULL,
    REAL_ELAPSED_TIME integer NOT NULL,
    AIR_TIME integer NOT NULL,
    CANCELLED boolean NOT NULL,
    CANCELLED_REASON text NOT NULL,
    CARRIER_DELAY smallint NOT NULL,
    WEATHER_DELAY smallint NOT NULL,
    NAS_DELAY smallint NOT NULL,
    SECURITY_DELAY smallint NOT NULL,
    LATE_AIRCRAFT_DELAY smallint NOT NULL,
    SPEED float NOT NULL,

    PRIMARY KEY (FLIGHT_ID),
    FOREIGN KEY (DATE) REFERENCES Date(DATE),
    FOREIGN KEY (AIRLINE_CODE) REFERENCES Airline(AIRLINE_CODE),
    FOREIGN KEY (ARRIVAL_AIRPORT) REFERENCES Airport(AIRPORT_CODE),
    FOREIGN KEY (DEPARTURE_AIRPORT) REFERENCES Airport(AIRPORT_CODE)
)"""
)
conn = connect()
cur = conn.cursor()
for create in creates:
    cur.execute(create)
conn.commit()
cur.close()

flights = pd.read_csv('final.csv')
airlines = pd.read_csv('airlines.csv')
airlines = airlines.dropna()
airports = pd.read_csv('airports_full.csv')

#Airlines

flights = pd.read_csv('final.csv')
airlines = pd.read_csv('airlines.csv')
airlines = airlines.dropna()
airports = pd.read_csv('airports_full.csv')

sql_airline = """
    INSERT INTO AIRLINE (
        AIRLINE_CODE,
        NAME,
        COUNTRY,
        ACTIVE
    ) VALUES"""
    
    conn = connect()
cursor = conn.cursor()
values = [cursor.mogrify("(%s, %s, %s, %s)", tup).decode('utf8') for tup in tuples_airline]
query  = sql_airline + ",".join(values) + " ON CONFLICT DO NOTHING"
cursor.execute(query)
conn.commit()
cursor.close()

#Airports

tuples_airport = []
for index, row in airports.iterrows():
    tuples_airport.append((
        row['iata_code'],
        row['name'],
        row['COUNTRY'],
        row['STATE'],
        row['municipality'],
        row['type'],
        float(row['longitude']),
        float(row['latitude']),
        int(row['elevation_ft']/3.2808),
    ))
    
sql_airport = """INSERT INTO AIRPORT (
        AIRPORT_CODE,
        NAME,
        COUNTRY,
        STATE,
        CITY,
        TYPE,
        LONGITUDE,
        LATITUDE,
        ELEVATION
    ) VALUES"""
    
conn = connect()
cursor = conn.cursor()
values = [cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", tup).decode('utf8') for tup in tuples_airport]
query  = sql_airport + ",".join(values) + " ON CONFLICT DO NOTHING"
cursor.execute(query)
conn.commit()
cursor.close()

#Dates 

tuples_date = []

for index, row in flights.iterrows():
    minutes = str(int(row['CRS_DEP_TIME']))
    minutes = "0" * (4 - len(minutes)) + minutes
    date = row['FL_DATE'] + " " + minutes[:2] + ":" + minutes[2:]
    datet = datetime.strptime(date, '%Y-%m-%d %H:%M')
    year = datet.year
    month = datet.month
    day = datet.day
    hours = datet.hour
    minutes = datet.minute
    
    seasons = {
        1 : 1,
        2 : 1,
        3 : 2,
        4 : 2,
        5 : 2,
        6 : 3,
        7 : 3,
        8 : 3,
        9 : 4,
        10 : 4,
        11 : 4,
        12 : 1
    }
    season = seasons[month]

    tuples_date.append(( 
        str(date),
        int(year),
        int(month),
        int(day),
        int(season),
        int(hours),
        int(minutes),
    ))
    
tuples_date = list(dict.fromkeys(tuples_date))

sql_date = """
    INSERT INTO DATE (
        DATE,
        YEAR,
        MONTH,
        DAY,
        SEASON,
        HOURS,
        MINUTES
    ) VALUES"""
    
for i in range(0, len(tuples_date), 50000):
    start = i
    end = i+50000
    conn = connect()
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s)", tup).decode('utf8') for tup in tuples_date[start:end]]
    query  = sql_date + ",".join(values) + " ON CONFLICT DO NOTHING"
    cursor.execute(query)
    conn.commit()
    cursor.close()
    print("Commited: " +str(start)+" to "+str(end))
    
    
#Flights

def speed(distance, air_time):
    if air_time==0 : 
        return 0
    else: 
        return (round(distance/air_time * 60))
        
def zeroifna(val):
    if np.isnan(val):
        return 0
    else : return val
    
    
tuples_flight = []
for index, row in flights.iterrows():
    minutes = str(int(row['CRS_DEP_TIME']))
    minutes = "0" * (4 - len(minutes)) + minutes
    date = row['FL_DATE'] + " " + minutes[:2] + ":" + minutes[2:]

    tuples_flight.append((
        str(uuid.uuid4()),
        date,
        row['OP_CARRIER'],
        row['DEST'],
        row['ORIGIN'],
        row['DISTANCE'],
        int(row['CRS_DEP_TIME']),
        int(zeroifna(row['DEP_TIME'])),
        int(zeroifna(row['DEP_DELAY'])),
        int(row['CRS_ARR_TIME']),
        int(zeroifna(row['ARR_TIME'])),
        int(zeroifna(row['ARR_DELAY'])),
        int(zeroifna(row['CRS_ELAPSED_TIME'])),
        int(zeroifna(row['ACTUAL_ELAPSED_TIME'])),
        int(zeroifna(row['AIR_TIME'])),
        bool(row['CANCELLED']),
        row['CANCELLATION_CODE'],
        int(zeroifna(row['CARRIER_DELAY'])),
        int(zeroifna(row['WEATHER_DELAY'])),
        int(zeroifna(row['NAS_DELAY'])),
        int(zeroifna(row['SECURITY_DELAY'])),
        int(zeroifna(row['LATE_AIRCRAFT_DELAY'])),
        speed(row['DISTANCE'], zeroifna(row['AIR_TIME']))
    ))
    
sql_flight = """
    INSERT INTO FLIGHT (
        FLIGHT_ID,
        DATE,
        AIRLINE_CODE,
        ARRIVAL_AIRPORT,
        DEPARTURE_AIRPORT,
        DISTANCE,
        EST_DEP_HOUR,
        REAL_DEP_HOUR,
        DEP_DELAY, 
        EST_ARR_HOUR,
        ARR_HOUR,
        ARR_DELAY,
        EST_ELAPSED_TIME,
        REAL_ELAPSED_TIME,
        AIR_TIME,
        CANCELLED,
        CANCELLED_REASON,
        CARRIER_DELAY,
        WEATHER_DELAY,
        NAS_DELAY,
        SECURITY_DELAY,
        LATE_AIRCRAFT_DELAY,
        SPEED
    ) VALUES"""

for i in range(0, len(tuples_flight), 50000):
    start = i
    end = i+50000
    conn = connect()
    cursor = conn.cursor()
    values = [cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tup).decode('utf8') for tup in tuples_flight[start:end]]
    query  = sql_flight + ",".join(values) + " ON CONFLICT DO NOTHING"
    cursor.execute(query)
    conn.commit()
    cursor.close()
    print("Commited: " +str(start)+" to "+str(end))
    
