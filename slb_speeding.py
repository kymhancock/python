#import packages
import datetime
import mysql.connector as connector

#define variables
account_id = 945
d = datetime.datetime
midnight = d.combine(d.today(), datetime.time.min)
start_date = d.combine(midnight - datetime.timedelta(days=30), datetime.time.min).strftime('%Y-%m-%d')
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
def speeding_violations(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading speeding_violations'

    query = """select basin, segment, aggMonth
                , sum(speeding_Violations_SBS) speeding_Violations_SBS
                , sum(speeding_Miles_SBS) speeding_Miles_SBS
                , sum(speeding_Violations_Threshold) speeding_Violations_Threshold
                , sum(speeding_Miles_Threshold) speeding_Miles_Threshold
                from (
                select org.basin, org.segment, org.location, org.group_path
                        , date_format(convert_TZ(n.time, 'GMT', dr.tzName), '%Y-%m-01') aggMonth
                        , sum(case when getAttrValue(65,attrs) = 3 then 1 else 0 end) speeding_Violations_SBS
                        , sum(case when getAttrValue(65,attrs) = 3 then n.distance/100 else 0 end) speeding_Miles_SBS
                        , sum(case when getAttrValue(65,attrs) = 2 then 1 else 0 end) speeding_Violations_Threshold
                        , sum(case when getAttrValue(65,attrs) = 2 then n.distance/100 else 0 end) speeding_Miles_Threshold
                from dataservices_scratch.dmarcus_SLB_OrgGroup org
                        join driver dr
                                on org.group_path = dr.group_fullname
                                        and dr.account_id = %s
                        join allnote n
                                on dr.driver_id = n.driver_id
                                        and n.account_id = %s
                                        and n.type in (4,49,58,93,191)
                                        and n.time >= %s - interval 1 day
                                        and n.time < %s + interval 2 day
                                        and getAttrValue(65,attrs) in (2,3)
                        join vehicle v
                                on n.vehicle_id = v.vehicle_id

                group by 1, 2, 3

                union
                select org.basin, org.segment, org.location, org.group_path
                        , date_format(convert_TZ(n.time, 'GMT', dr.tzName), '%Y-%m-01') aggMonth
                        , sum(case when getAttrValue(65,attrs) = 3 then 1 else 0 end) speeding_Violations_SBS
                        , sum(case when getAttrValue(65,attrs) = 3 then n.distance/100 else 0 end) speeding_Miles_SBS
                        , sum(case when getAttrValue(65,attrs) = 2 then 1 else 0 end) speeding_Violations_Threshold
                        , sum(case when getAttrValue(65,attrs) = 2 then n.distance/100 else 0 end) speeding_Miles_Threshold
                from dataservices_scratch.dmarcus_SLB_OrgGroup org
                        join vehicle v
                                on org.group_path = v.group_fullname
                                        and v.account_id = %s
                        join allnote n
                                on v.vehicle_id = n.vehicle_id
                                        and n.account_id = %s
                                        and n.type in (4,49,58,93,191)
                                        and n.time >= %s
                                        and n.time < %s
                                        and getAttrValue(65,attrs) in (2,3)
                        join driver dr
                                on n.driver_id = dr.driver_id
                                      and dr.name='No Driver'
                group by 1, 2, 3
                ) a
                where aggMonth >= %s
                and aggMonth < %s
                group by 1, 2, 3;"""

    args = [account_id, account_id, start_date, end_date, account_id, account_id, start_date, end_date, start_date, end_date]
    cur.execute(query, args)

    print 'inserting rows into dataservices_scratch.slb_speeding'

    results = cur.fetchall()
    for row in results:
        basin = row[0]
        segment = row[1]
        aggMonth = row[2]
        Speeding_Violations_SBS = row[3]
        Speeding_Miles_SBS = row[4]
        speeding_Violations_Threshold = row[5]
        speeding_Miles_Threshold = row[6]

        cur.execute("""delete from dataservices_scratch.slb_speeding where aggMonth < '2017-01-01'""")

        cur.execute("""insert into dataservices_scratch.slb_speeding
                    (basin, segment, aggMonth, Speeding_Violations_SBS, Speeding_Miles_SBS, speeding_Violations_Threshold, speeding_Miles_Threshold)
                    values (%s, %s, %s, %s, %s, %s, %s)""", (basin, segment, aggMonth, Speeding_Violations_SBS, Speeding_Miles_SBS, speeding_Violations_Threshold, speeding_Miles_Threshold))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

speeding_violations(account_id, start_date, end_date)
