#!/usr/bin/python
#-*- coding: utf-8 -*-　　 
#-*- coding: cp950 -*-　
import psycopg2

# read file
f = open("Speed-Measuring Sensor", "r")
conn = psycopg2.connect(database="db2", user="db2", password="db002", host="140.114.77.23", port="5432")
print "Opened database successfully"
cur = conn.cursor()
'''
f.readline()
f.readline()
for line in f.readlines():
    line = line.split()
    cur.execute("INSERT INTO speed_measuring_sensor (Item_NO,Road_ID,Road_Direction,Milage,Speed_Limit,Latitude,Longitude) VALUES ("+line[0]+", '"+line[2]+"', '"+line[3]+"', "+line[4]+", "+line[5]+", "+line[6]+", "+line[7]+")")
'''
conn.commit()
cur.execute("SELECT * FROM speed_measuring_sensor")
rows = cur.fetchall()
print rows

