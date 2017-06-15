#!/usr/bin/python
#-*- coding: utf-8 -*-　　 
#-*- coding: cp950 -*-　
import psycopg2

# read file
f = open("Speed-Measuring-Sensor_1", "r")
conn = psycopg2.connect(database="db2", user="db2", password="db002", host="140.114.77.23", port="5432")
print "Opened database successfully"
cur = conn.cursor()
f.readline()
for line in f.readlines():
    line = line.split()
    #cur.execute("INSERT INTO speed_measuring_sensor (Item_NO,Road_ID,Road_Direction,Milage,Speed_Limit,Latitude,Longitude) VALUES ("+line[0]+", '"+line[2]+"', '"+line[3]+"', "+line[4]+", "+line[5]+", "+line[6]+", "+line[7]+")")
    cur.execute("UPDATE speed_measuring_sensor set location_path =" + line[8] + " where Item_No="+line[0])
conn.commit()
cur.execute("SELECT * FROM speed_measuring_sensor")
rows = cur.fetchall()
for row in rows:
    print (row)

