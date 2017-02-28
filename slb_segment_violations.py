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

def mileage(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading mileage'


    query = """select basin, segment, aggMonth
               , sum(drivers) drivers
               , sum(total_miles) total_miles
               , sum(drivers_not_heavy_vehicles) drivers_not_heavy_vehicles
               , sum(total_miles_not_heavy_vehicles) total_miles_not_heavy_vehicles
                    from (
                        select org.basin, org.segment
                        , date_format(agg.aggDate, '%Y-%m-01') aggMonth
                        , count(distinct dr.driver_id) drivers
                        , sum(agg.odometer6/100) total_miles

                        , sum(case when v.vtype in (0,1) then 1 else 0 end) drivers_not_heavy_vehicles
                        , sum(case when v.vtype in (0,1) then agg.odometer6/100 else 0 end) total_miles_not_heavy_vehicles

                            from dataservices_scratch.dmarcus_SLB_OrgGroup org
                            join driver dr
                            on org.group_path = dr.group_fullname
                                and dr.account_id = %s
                            join agg
                                on dr.driver_id = agg.driver_id
                            join days
                                on agg.aggDate = days.aggDate
                                    and days.aggDate >= %s - interval 1 day
                                    and days.aggDate < %s + interval 2 day
                            join vehicle v
                                on agg.vehicle_id = v.vehicle_id
                                    group by 1, 2, 3

                            union

                        select org.basin, org.segment
                        , date_format(agg.aggDate, '%Y-%m-01') aggMonth
                        , count(distinct dr.driver_id) drivers
                        , sum(agg.odometer6/100) total_miles

                        , sum(case when v.vtype in (0,1) then 1 else 0 end) drivers_not_heavy_vehicles
                        , sum(case when v.vtype in (0,1) then agg.odometer6/100 else 0 end) total_miles_not_heavy_vehicles

                            from dataservices_scratch.dmarcus_SLB_OrgGroup org
                            join vehicle v
                            on org.group_path = v.group_fullname
                                and v.account_id = %s
                            join agg
                                on v.vehicle_id = agg.vehicle_id
                            join driver dr
                                on agg.driver_id = dr.driver_id
                            join days
                                on agg.aggDate = days.aggDate
                                and days.aggDate >= %s - interval 1 day
                                and days.aggDate < %s + interval 2 day
                                    where dr.name='No Driver'
                                    group by 1, 2, 3) a
                                    group by 1, 2, 3;"""

    args = [account_id, start_date, end_date, account_id, start_date, end_date]
    cur.execute(query, args)
    print 'inserting rows into dataservices_scratch.slb_mileage'

    results = cur.fetchall()
    for row in results:
        basin = row[0]
        segment = row[1]
        aggMonth = row[2]
        driver_count = row[3]
        total_miles = row[4]
        driver_count_non_heavy_vehicles = row[5]
        total_miles_non_heavy_vehicles = row[6]

        cur.execute("""delete from dataservices_scratch.slb_mileage where aggMonth < '2017-01-01'""")

        cur.execute("""insert into dataservices_scratch.slb_mileage
                    (basin, segment, aggMonth, driver_count, total_miles, driver_count_non_heavy_vehicles, total_miles_non_heavy_vehicles)
                    values (%s, %s, %s, %s, %s, %s, %s)""", (basin, segment, aggMonth, driver_count, total_miles, driver_count_non_heavy_vehicles, total_miles_non_heavy_vehicles))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

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

def speeding_violations(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading speeding violations'

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

def unknown_driver_miles(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading unknown_driver_miles'

    query = """select org.basin, org.segment
        , date_format(agg.aggDate, '%Y-%m-01') aggMonth
        , count(distinct dr.name) drivers
        , sum(agg.odometer6/100) total_miles
            from dataservices_scratch.dmarcus_SLB_OrgGroup org
            join vehicle v
                on org.group_path = v.group_fullname
                        and v.account_id = %s
            join agg
                on v.vehicle_id = agg.vehicle_id
            join driver dr
                on agg.driver_id = dr.driver_id
            join days
                on agg.aggDate = days.aggDate
                and days.aggDate >= %s - interval 1 day
                and days.aggDate < %s + interval 2 day
                    where dr.name='No Driver'
                    group by 1, 2, 3;"""

    args = [account_id, start_date, end_date]
    cur.execute(query, args)
    results = cur.fetchall()
    print 'inserting rows into dataservices_scratch.slb_mileage'

    for row in results:
        basin = row[0]
        segment = row[1]
        aggMonth = row[2]
        drivers = row[3]
        total_miles = row[4]


        cur.execute("""delete from dataservices_scratch.slb_unknown_driver where aggMonth < '2017-01-01'""")

        cur.execute("""insert into dataservices_scratch.slb_unknown_driver
                    (basin, segment, aggMonth, drivers, total_miles)
                    values (%s, %s, %s, %s, %s)""", (basin, segment, aggMonth, drivers, total_miles))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

def non_comm_90days(account_id):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading non_comm_90days'

    query = """select org.basin, org.segment
        , curdate() - interval 3 month as date_range_start
        , curdate() as date_range_end
        , sum(case when ddns.device_Id is null then 1 else 0 end) non_Comm
                from device d
                join vehicle v on d.device_Id = v.device_Id
                join dataservices_scratch.dmarcus_SLB_OrgGroup org on v.group_fullname = org.group_path
                left join (
                        select device_Id
                        , min(aggDate) aggDate
                                from device_daily_note_summary
                                where account_id = %s
                                and aggDate >= curdate() - interval 3 month
                                and aggDate < curdate()
                                        group by 1) ddns on d.device_id = ddns.device_id
                                        where d.account_Id=%s
                                        and d.status_Id=1
                                        group by 1, 2;"""

    args = [account_id, account_id]
    cur.execute(query, args)
    results = cur.fetchall()

    print 'inserting rows into dataservices_scratch.slb_noncomm90'

    for row in results:
        basin = row[0]
        segment = row[1]
        date_range_start = row[2]
        date_range_end = row[3]
        non_Comm = row[4]

        cur.execute("""delete from dataservices_scratch.slb_noncomm90 where date_range_start < curdate() - interval 3 month""")

        cur.execute("""insert into dataservices_scratch.slb_noncomm90
                    (basin, segment, date_range_start, date_range_end, non_Comm)
                    values (%s, %s, %s, %s, %s)""", (basin, segment, date_range_start, date_range_end, non_Comm))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

def no_grps_but_sat(account_id, start_date, end_date):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading no_grps_but_sat'

    query = """select basin
            , segment
            , aggMonth
            , sum(case when connect_gprs = 0 and connect_satellite > 0 then 1 else 0 end) no_gprs_comm
                    from (
                    select org.basin
                            , org.segment
                            , v.vehicle_id
                            , date_format(convert_TZ(ddns.aggDate, 'GMT', dr.tzName), '%Y-%m-01') aggMonth
                            , sum(case when ddns.connect_type_id = 1 then 1 else 0 end) connect_satellite
                            , sum(case when ddns.connect_type_id = 2 then 1 else 0 end) connect_gprs
                                    from dataservices_scratch.dmarcus_SLB_OrgGroup org
                                    join vehicle v on org.group_path = v.group_fullname and v.account_id = %s
                                    join device_daily_note_summary ddns on ddns.vehicle_id = v.vehicle_id
                                            and ddns.account_id = %s
                                            and ddns.aggDate >= %s
                                            and ddns.aggDate < %s
                                    join driver dr on ddns.driver_id = dr.driver_id
                                            group by 1, 2, 3, 4) a
                                            where aggMonth >= %s
                                            and aggMonth < %s
                                                    group by 1, 2, 3;"""

    args = [account_id, account_id, start_date, end_date, start_date, end_date]
    cur.execute(query, args)
    results = cur.fetchall()
    print 'inserting rows into dataservices_scratch.slb_no_gprs'

    for row in results:
        basin = row[0]
        segment = row[1]
        aggMonth = row[2]
        no_gprs_comm = row[3]

        cur.execute("""delete from dataservices_scratch.slb_no_gprs where aggMonth < '2017-01-01'""")

        cur.execute("""insert into dataservices_scratch.slb_no_gprs
                    (basin, segment, aggMonth, no_gprs_comm)
                    values (%s, %s, %s, %s)""", (basin, segment, aggMonth, no_gprs_comm))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

def seatbelt_violations_3mo(account_id):
    cn = connection()
    cur = cn.cursor()
    fileDate = start_date
    print 'loading seatbelt_violations_3mo'

    query = """select (curdate() - interval 90 day) as date_range_start
            , curdate() as date_range_end
            , org.basin
            , org.segment
            , dr.name as driver
            , count(*) seatbelt_Violations
            , sum(n.distance/100) as seatbelt_Violation_Miles
                    from dataservices_scratch.dmarcus_SLB_OrgGroup org
                    join driver dr on org.group_path = dr.group_fullname and dr.account_id = %s
                            join note n
                                    on dr.driver_id = n.driver_id
                                            and n.account_id = %s
                                            and n.type = 3
                                            and n.time >= curdate() - interval 90 day
                                            and n.time < curdate()
                                            and n.forgiven = 0
                                                    group by 3, 4, 5
                                                    order by 3, 4, 5;"""

    args = [account_id, account_id]
    cur.execute(query, args)
    results = cur.fetchall()
    print 'inserting rows...'

    for row in results:
        date_range_start = row[0]
        date_range_end = row[1]
        basin = row[2]
        segment = row[3]
        driver = row[4]
        seatbelt_Violations = row[5]
        seatbelt_Violation_Miles = row[6]

        cur.execute("""delete from dataservices_scratch.slb_seatbelt_3mo where date_range_start < curdate() - interval 3 month""")

        cur.execute("""insert into dataservices_scratch.slb_seatbelt_3mo
                    (date_range_start, date_range_end, basin, segment, driver, seatbelt_Violations, seatbelt_Violation_Miles)
                    values (%s, %s, %s, %s, %s, %s, %s)""", (date_range_start, date_range_end, basin, segment, driver, seatbelt_Violations, seatbelt_Violation_Miles))

        cn.commit()

    print 'committing to database'
    cur.close()
    cn.close()

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
mileage(account_id, start_date, end_date)
seatbelt_violations(account_id, start_date, end_date)
speeding_violations(account_id, start_date, end_date)
unknown_driver_miles(account_id, start_date, end_date)
non_comm_90days(account_id)
no_grps_but_sat(account_id, start_date, end_date)
seatbelt_violations_3mo(account_id)
speeding_drivers_3mo(account_id)
