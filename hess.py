#import packages
import datetime
import mysql.connector as connector
import pandas as pd
import os
import zipfile
import csv

#define variables
account_id = '669339'
d = datetime.datetime
midnight = d.combine(d.today(), datetime.time.min)
start_date = d.combine(midnight - datetime.timedelta(days=5), datetime.time.min).strftime('%Y-%m-%d')
end_date = d.today().strftime('%Y-%m-%d')

#define connection
def connection():
   
    config = {
        "user": "salesreports",
        "password": "Eiseisah5Ziec6shighi1xa4foo15vP",
        "host" : "data.tiwii.com",
        "database": "salesreports"}
    try:
        c = connector.connect(**config)
        return c
    except:
        print "connection error " + connector.connect(**config)
        exit(1)
    cn = connection()
    cur = cn.cursor()


#define functions

def getViolations(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    csvFileName = 'violations.csv'
    print 'loading notes\n'

    query = """select *
               from (
                       select convert_TZ(n.time, 'GMT', dr.tzName) date
                               , n.noteID
                               , g.remote_group_id group_id
                               , g.name group_name
                               , dr.group_fullname
                               , dr.remote_driver_id driver_id
                               , dr.empid
                               , dr.name driver_name
                               , v.remote_vehicle_id vehicle_id
                               , v.name vehicle_name
                               , concat(
                                       nn.name
                                       , case when n.type in (2,171) then '- ' else '' end
                                       , ifnull(case when n.type in (2,171) then getStyleType(n.type,n.deltaX,n.deltaY,n.deltaZ) else cast('' as char(45)) end, '')
                                       )
                                        event_name
                               , n.speed
                               , n.topSpeed
                               , n.avgSpeed
                               , n.speedLimit
                               , n.distance/100 distance
                               , n.latitude
                               , n.longitude
                               , n.forgiven
                       from driver dr
                               join note n
                                       on dr.driver_id = n.driver_id
                               join vehicle v
                                       on n.vehicle_Id = v.vehicle_id
                               join groups g
                                       on dr.group_id = g.group_id
                               join noteNames nn
                                       on n.type = nn.type
                                       where dr.account_id = """+account_id+"""
                                         and n.time >= '"""+start_date+"""' - interval 1 day
                                         and n.time <= '"""+end_date+"""'  + interval 2 day
                                         and n.type in (2,3,4,49,58,93,171,191)
                                         ) a
                                            where a.date >= '"""+start_date+"""'
                                            and a.date < '"""+end_date+"""' + interval 1 day
                                              order by 1;"""
    print(query)
    cur.execute(query)
    results = cur.fetchall()

    with open(csvFileName, 'w') as output:
        a = csv.writer(output, delimiter= ',')
	a.writerows(results)
    if cur.rowcount != 0:
        print('writing results to ' + csvFileName)
    else:
        print(str(connector.Error('Could not write results to .csv file')))
    cur.close()
    cn.close()


def getForgiven(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    csvFileName = 'forgiven.csv'
    print 'loading forgiven\n'

    query = """select fh.remote_Note_Id noteID, fh.time, fh.oldState, fh.newState
                                 from forgiven_history fh
                                 where fh.account_id= """+account_id+"""
                                         and time >='"""+start_date+"""'
                                         and time <'"""+end_date+"""' + interval 1 day
                                 order by 1;"""

    print(query)
    cur.execute(query)
    results = cur.fetchall()

    with open(csvFileName, 'w') as output:
        a = csv.writer(output, delimiter = ',')
        a.writerows(results)
    if cur.rowcount != 0:
        print('writing results to ' + csvFileName)
    else:
        print(str(connector.Error('Could not write results to .csv file')))
    cur.close()
    cn.close()

def getAgg(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    csvFileName = 'agg.csv'

    print 'loading agg\n'

    query = """select agg.aggDate
                                 , v.remote_vehicle_id vehicle_id
                                 , v.name vehicle_name
                                 , v.group_fullname
                                 , dr.remote_driver_id driver_id
                         , dr.empid
                         , dr.name driver_name
                         , (agg.mpgOdometer1+agg.mpgOdometer2+agg.mpgOdometer3)/100 as mpgMiles
                         , (agg.mpgGal1+agg.mpgGal2+agg.mpgGal3)/1000 mpgGallons
                         , round(10*(mpgOdometer1+mpgOdometer2+mpgOdometer3)/(mpgGal1+mpgGal2+mpgGal3),2) as mpg
                         , driveTime as driveTime
                         , idleLo as idleLo
                         , idleHi as idleHi
                         , (odometer1+odometer2+odometer3+odometer4+odometer5)/100 as milesDriven
                 from agg
                         join vehicle v
                                 on agg.vehicle_id = v.vehicle_Id
                         join driver dr
                                 on agg.driver_id = dr.driver_Id
                 where agg.account_id= """+account_id+"""
                         and agg.aggDate >='"""+start_date+"""'
                         and agg.aggDate < '"""+end_date+"""' + interval 1 day
                         and agg.odometer6 > 0
                 order by 1;"""

    print(query)
    cur.execute(query)
    results = cur.fetchall()
 
    with open(csvFileName, 'w') as output:
        a = csv.writer(output, delimiter = ',')
        a.writerows(results)

    if cur.rowcount != 0:
        print('writing results to ' + csvFileName)
    else:
        print(str(connector.Error('Could not write results to .csv file')))
    cur.close()
    cn.close()

#call functions
getViolations(account_id, start_date, end_date)
getForgiven(account_id, start_date, end_date)
getAgg(account_id, start_date, end_date)
