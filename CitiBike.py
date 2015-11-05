import requests

r = requests.get('http://www.citibikenyc.com/stations/json')

import matplotlib.pyplot as plt
import pandas as pd


key_list = []
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

from pandas.io.json import json_normalize

df = json_normalize(r.json()['stationBeanList'])

stations =r.json()["stationBeanList"]

import collections
print collections.Counter(station["statusValue"] for station in stations)
print collections.Counter(station["testStation"] for station in stations)

print df['totalDocks'].mean()
print df['totalDocks'].median()

condition = (df['statusValue'] == 'In Service')
print df[condition]['totalDocks'].mean()

df['totalDocks'].median()
print df[df['statusValue'] == 'In Service']['totalDocks'].median()


import time
from dateutil.parser import parse 
import collections
import sqlite3 as lite
import requests

con = lite.connect('citi_bike.db')
cur = con.cursor()

with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')

sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

with con:
    for station in r.json()['stationBeanList']:
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

station_ids = df['id'].tolist() 

station_ids = ['_' + str(x) + ' INT' for x in station_ids] 

with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

for i in range(60):
    r = requests.get('http://www.citibikenyc.com/stations/json')
    exec_time = parse(r.json()['executionTime'])

    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%Y-%m-%dT%H:%M:%S'),))
    con.commit()

    id_bikes = collections.defaultdict(int)
    for station in r.json()['stationBeanList']:
        id_bikes[station['id']] = station['availableBikes']

    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime("'%Y-%m-%dT%H:%M:%S'") + ";")
    con.commit()

    time.sleep(60)

con.close() 

import pandas as pd
import collections
import sqlite3 as lite
import datetime

con = lite.connect('citi_bike.db')
cur = con.cursor()

df = pd.read_sql_query("SELECT * FROM available_bikes ORDER BY execution_time",con,index_col='execution_time')

hour_change = collections.defaultdict(int)
for col in df.columns:
    station_vals = df[col].tolist()
    station_id = col[1:]
    station_change = 0
    for k,v in enumerate(station_vals):
        if k < len(station_vals) - 1:
            station_change += abs(station_vals[k] - station_vals[k+1])
    hour_change[int(station_id)] = station_change 

def keywithmaxval(d):
	return max(d, key=lambda k: d[k])


max_station = keywithmaxval(hour_change)

cur.execute("SELECT id, stationname, latitude, longitude FROM citibike_reference WHERE id = ?", (max_station,))
data = cur.fetchone()
print "The most active station is station id %s at %s latitude: %s longitude: %s " % data
print "With " + str(hour_change[max_station]) + " bicycles coming and going in the hour between " + df.index[0] + " and " + df.index[-1]

import matplotlib.pyplot as plt

plt.bar(hour_change.keys(), hour_change.values())
plt.show()