__author__ = 'popmigration'
import sys
import csv
import string

# Check parameters.  Offer help.
username = 'root'
password = 'password'
hostname = 'localhost'

numargs = len(sys.argv)
if numargs >= 2:
    # Get CSV input filename from command line
    csvfilename = sys.argv[1]
if numargs >= 3:
    # Get year parameter from command line
    selected_year = sys.argv[2]
if numargs >= 4:
    # Get database user from command line - default is root
    username = sys.argv[3]
if numargs >= 5:
    # Get database password from command line - default is password
    password = sys.argv[4]
if numargs >= 6:
    # Get database server host from command line - default is localhost
    hostname = sys.argv[5]
if (6 < numargs) or (numargs < 3):
    # Output Error message and exit
    print("Usage: csv2db <csv-fileName> <4-digit-year> [dbusername] [dbpassword] [dbhostname]\n")
    sys.exit(0)

from sqlalchemy import create_engine, select, and_, Table, Index, Column, Integer, String, MetaData, ForeignKey
metadata = MetaData()

locationCodes = Table("location_codes", metadata,
                      Column("code", String(40), primary_key=True),
                      Column("stateCode", Integer),
                      Column("countyCode", Integer),
                      Column("stateAbbr", String(2)),
                      Column("countyName", String(37)),
)

Index("locationcode_index", locationCodes.c.stateCode, locationCodes.c.countyCode)

detail = Table("detail", metadata,
                     Column("id", Integer, primary_key=True),
                     Column("destCode", None, ForeignKey("location_codes.code")),
                     Column("originCode", None, ForeignKey("location_codes.code")),
                     Column("numReturns", Integer, nullable=False),
                     Column("numDependents", Integer, nullable=False),
                     Column("aggregateAGI", Integer, nullable=False),
                     Column("year", Integer, nullable=False),
)

summary = Table("summary", metadata,
                 Column("id", Integer, primary_key=True),
                 Column("locCode", None, ForeignKey("location_codes.code")),
                 Column("summaryType", Integer, nullable=False),  # Origin State Code
                 Column("numReturns", Integer, nullable=False),
                 Column("numDependents", Integer, nullable=False),
                 Column("aggregateAGI", Integer, nullable=False),
                 Column("year", Integer, nullable=False),
)

dburl = 'mysql://' + username + ':' + password + '@' + hostname + '/popmigration?charset=utf8&use_unicode=0'
engine = create_engine(dburl, pool_recycle=3600)
metadata.create_all(engine)
conn = engine.connect()

# Clear any previously loaded data for the specified year from database



# Find location code WHERE state and county match
def lookupLocationCode(stateCode, countyCode, stateAbbr, countyName):
    locations = select([locationCodes]).where(and_(locationCodes.c.stateCode == stateCode, locationCodes.c.countyCode == countyCode))
    locations_result = conn.execute(locations)
    if 0 == locations_result.rowcount:
        # Location does not yet exist -> insert one
        shortStateName = stateAbbr[:2]
        # Remove " Non-migrants" from County Name

        shortCountyName = countyName.replace("Non-migrants", "")
        shortCountyName = shortCountyName.replace("Non-Migrants", "")
        shortCountyName = shortCountyName[:37].strip()
        result = shortStateName + "-" + string.capwords(shortCountyName)
        insert_location = locationCodes.insert()
        inserted_location = conn.execute(insert_location, code=result, stateCode=stateCode, countyCode=countyCode,
                                         stateAbbr=stateAbbr, countyName=countyName)
        if not inserted_location:
            result = None
    elif 1 == locations_result.rowcount:
        # Unique Result -> return location code
        sqlrow = locations_result.fetchone()
        result = sqlrow["code"]
    else:
        # More than one row returned - should not occur -> show error and exit.
        print("Multiple of this location found in file!")
        sys.exit(-1)
    locations_result.close()
    return result


def shouldInclude(rowparm):
    return int(rowparm['State_Code_Origin']) <= 57



# Code: Delete all rows from data table and summary table with specified year.

with open(csvfilename) as csvfile:
    # Map CSV column names to positions
    irscsvreader = csv.DictReader(csvfile, delimiter=',')

    # Iterate over rows in source CSV file, add locations as needed and store data in SQL tables
    for csvrow in irscsvreader:

        # Lookup/Create a location code for the specified origin state and county numeric codes
        originCode = lookupLocationCode(csvrow['State_Code_Origin'], csvrow['County_Code_Origin'], csvrow['State_Abbrv'],
                                        csvrow['County_Name'])
        if shouldInclude(csvrow):
            # CSV row is detail data - so store in detail table

            # Lookup/Create a location code for the specified destination state and county numeric codes
            destCode = lookupLocationCode(csvrow['State_Code_Dest'], csvrow['County_Code_Dest'], csvrow['State_Abbrv'],
                                          csvrow['County_Name'])
            insert_data = detail.insert()
            inserted_data = conn.execute(insert_data, destCode=destCode, originCode=originCode,
                                         numReturns=csvrow['Return_Num'], numDependents=csvrow['Exmpt_Num'],
                                         aggregateAGI=csvrow['Aggr_AGI'], year=selected_year)
            print("Inserted detail")
        else:
            # CSV row is summary data - so store in summary table
            insert_data = summary.insert()
            inserted_data = conn.execute(insert_data, locCode=originCode, summaryType=csvrow['State_Code_Origin'],
                                         numReturns=csvrow['Return_Num'], numDependents=csvrow['Exmpt_Num'],
                                         aggregateAGI=csvrow['Aggr_AGI'], year=selected_year)
            print("Inserted summary")


