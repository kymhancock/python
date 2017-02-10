#import packages
import pgdb
import csv

#define variables
geo_code = 'US-FL'

#define connection
def connection():

    config = {
        #!/usr/bin/python
            hostname = 'localhost'
            username = 'proprod'
            password = 'du0Oifo5aeth9ei'
            database = 'iwigis'}
    try:
        c = pgdb.connect(**config)
        return c
    except:
        print "connection error " + pgdb.connect(**config)
        exit(1)
    cn = connection()
    cur = cn.cursor()

#define functions
def sbs_export(geo_code):
    cn = connection()
    cur = cn.cursor()
    csvFileName = 'sbs_export.csv'
    print 'loading results...\n'

    query = """select geo_code as state
                    , gid
                    , kph
                    , verified_status
                    , iwi_spd_lim as inthinc_speed
                    , msp_spd_lim as map_provider_speed
                    , modified_speed_or_verified as last_date_modified
                    , asText(the_geom) as geometry
                    , ramp as ramp
                            from streets
                            where (basemask & (1<<(15-1))) = (1<<(15-1))
                            and geo_code = %s
                            order by geo_code;"""

    args = [geo_code]
    cur.execute(query, args)
    results = cur.fetchall()

    with open(csvFileName, 'w') as output:
        a = csv.writer(output, delimiter = ',')
        a.writerows(results)
    if cur.rowcount != 0:
        print('writing results to ' + csvFileName)
    else:
        print(str(pgdb.error('Could not write results to .csv file')))
    cur.close()
    cn.close()

#call functions
sbs_export(geo_code)
