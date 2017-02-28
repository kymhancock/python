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
def seatbelt_violations(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading seatbelt_violations'

    query = """select basin, segment, aggMonth
        , sum(seatbelt_Violations) seatbelt_Violations
        , sum(seatbelt_Violation_Miles) seatbelt_Violation_Miles
        from (
        select org.basin, org.segment
                , date_format(convert_TZ(n.time, 'GMT', dr.tzName), '%Y-%m-01') aggMonth
                , count(*) seatbelt_Violations
                , sum(n.distance/100) as seatbelt_Violation_Miles

        from dataservices_scratch.dmarcus_SLB_OrgGroup org
                join driver dr
                        on org.group_path = dr.group_fullname
                                and dr.account_id = %s
                join allnote n
                        on dr.driver_id = n.driver_id
                                and n.account_id = %s
                                and n.type = 3
                                and n.distance >30
                                and n.time >= %s
                                and n.time < %s
                                and n.forgiven = 0
                join vehicle v
                        on n.vehicle_id = v.vehicle_id
                                and v.vtype in (0, 1)
        group by 1, 2, 3

        union
        select org.basin, org.segment
                , date_format(convert_TZ(n.time, 'GMT', dr.tzName), '%Y-%m-01') aggMonth
                , count(*) seatbelt_Violations
                , sum(n.distance/100) as seatbelt_Violation_Miles
        from dataservices_scratch.dmarcus_SLB_OrgGroup org
                join vehicle v
                        on org.group_path = v.group_fullname
                                and v.account_id = %s
                                and v.vtype in (0, 1)
                join allnote n
                        on v.vehicle_id = n.vehicle_id
                                and n.account_id = %s
                                and n.type = 3
                                and n.distance >30
                                and n.time >= %s - interval 1 day
                                and n.time < %s + interval 2 day
                                and n.forgiven = 0
                join driver dr
                        on n.driver_id = dr.driver_id
                              and dr.name='No Driver'
        group by 1, 2, 3
        ) a
        where aggMonth >= %s
        and aggMonth < %s
        group by 1, 2, 3
        ;"""

    args = [account_id, account_id, start_date, end_date, account_id, account_id, start_date, end_date, start_date, end_date]
    cur.execute(query, args)
    results = cur.fetchall()
    print 'inserting rows into dataservices_scratch.slb_seatbelt'

    for row in results:
        basin = row[0]
        segment = row[1]
        aggMonth = row[2]
        seatbelt_Violations = row[3]
        seatbelt_Violation_Miles = row[4]


        cur.execute("""delete from dataservices_scratch.slb_seatbelt where aggMonth < '2017-01-01'""")

        cur.execute("""insert into dataservices_scratch.slb_seatbelt
                    (basin, segment, aggMonth, seatbelt_Violations, seatbelt_Violation_Miles)
                    values (%s, %s, %s, %s, %s)""", (basin, segment, aggMonth, seatbelt_Violations, seatbelt_Violation_Miles))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

#call functions
seatbelt_violations(account_id, start_date, end_date)
