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

def speeding_drivers_3mo(account_id):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading speeding_drivers_3mo'

    query = """select (curdate() - interval 90 day) as date_range_start
            , curdate() as date_range_end
            , org.basin
            , org.segment
            , org.location
            , dr.name as driver
            , sum(case when getAttrValue(65,attrs) = 3 then 1 else 0 end) speeding_Violations_SBS
            , sum(case when getAttrValue(65,attrs) = 3 then n.distance/100 else 0 end) speeding_Miles_SBS
            , sum(case when getAttrValue(65,attrs) = 2 then 1 else 0 end) speeding_Violations_Threshold
            , sum(case when getAttrValue(65,attrs) = 2 then n.distance/100 else 0 end) speeding_Miles_Threshold
            , sum(case when getAttrValue(65,attrs) = 3 then n.distance/100 else 0 end) + sum(case when getAttrValue(65,attrs) = 2 then n.distance/100 else 0 end) speeding_Total
                from dataservices_scratch.dmarcus_SLB_OrgGroup org
                join driver dr on org.group_path = dr.group_fullname and dr.account_id = %s
                join note n on dr.driver_id = n.driver_id
                        and n.account_id = %s
                        and n.type in (4,49,58,93,191)
                        and n.time >= curdate() - interval 90 day
                        and n.time < curdate()
                join vehicle v on n.vehicle_id = v.vehicle_id
                        group by 3, 4, 5, 6
                        having sum(case when getAttrValue(65,attrs) = 2 then n.distance/100 else 0 end) + sum(case when getAttrValue(65,attrs) = 3 then n.distance/100 else 0 end) >= 10
                        order by 3, 4, 5;"""


    args = [account_id, account_id]
    cur.execute(query, args)
    results = cur.fetchall()
    print 'inserting rows into dataservices_scratch.slb_speeding_3mo'

    for row in results:
        date_range_start = row[0]
        date_range_end = row[1]
        basin = row[2]
        segment = row[3]
        location = row[4]
        driver = row[5]
        speeding_Violations_SBS = row[6]
        speeding_Miles_SBS = row[7]
        speeding_Violations_Threshold = row[8]
        speeding_Miles_Threshold = row[9]
        speeding_Total = row[10]

        cur.execute("""delete from dataservices_scratch.slb_speeding_3mo where date_range_start < curdate() - interval 90 day""")

        cur.execute("""insert into dataservices_scratch.slb_speeding_3mo
                    (date_range_start, date_range_end, basin, segment, location, driver, speeding_Violations_SBS, speeding_Miles_SBS, speeding_Violations_Threshold, speeding_Miles_Threshold, speeding_Total)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (date_range_start, date_range_end, basin, segment, location, driver, speeding_Violations_SBS, speeding_Miles_SBS, speeding_Violations_Threshold, speeding_Miles_Threshold, speeding_Total))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

#call functions
speeding_drivers_3mo(account_id)
