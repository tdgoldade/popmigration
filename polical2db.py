__author__ = 'popmigration'
import sys
import csv
import os
import us
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
    # Get database user from command line - default is root
    username = sys.argv[2]
if numargs >= 4:
    # Get database password from command line - default is password
    password = sys.argv[3]
if numargs >= 5:
    # Get database server host from command line - default is localhost
    hostname = sys.argv[4]
if (5 < numargs) or (numargs < 2):
    # Output Error message and exit
    print("Usage: csv2db <csv-fileName> [dbusername] [dbpassword] [dbhostname]\n")
    sys.exit(0)

from sqlalchemy import create_engine, select, and_, Table, Index, Column, Numeric, Integer, String, MetaData, ForeignKey

metadata = MetaData()

locationCodes = Table("location_codes", metadata,
                      Column("code", String(40), primary_key=True),
                      Column("stateCode", Integer),
                      Column("countyCode", Integer),
                      Column("stateAbbr", String(2)),
                      Column("countyName", String(37)),
)

politicalCounty = Table("political_county", metadata,
                        Column("id", Integer, primary_key=True),
                        Column("locCode", None, ForeignKey("location_codes.code")),
                        Column("total_votes", Integer, nullable=False),
                        Column("rep_votes", Integer, nullable=False),
                        Column("dem_votes", Integer, nullable=False),
                        Column("rep_pct", Numeric(5, 2), nullable=False),
                        Column("dem_pct", Numeric(5, 2), nullable=False),
                        Column("year", Integer, nullable=False),
)

Index("polcounty_index", politicalCounty.c.locCode, politicalCounty.c.year)

politicalState = Table("political_state", metadata,
                       Column("id", Integer, primary_key=True),
                       Column("stateAbbr", None, ForeignKey("location_codes.code")),
                       Column("total_votes", Integer, nullable=False),
                       Column("rep_votes", Integer, nullable=False),
                       Column("dem_votes", Integer, nullable=False),
                       Column("rep_pct", Numeric(5, 2), nullable=False),
                       Column("dem_pct", Numeric(5, 2), nullable=False),
                       Column("year", Integer, nullable=False),
)

Index("polstate_index", politicalState.c.stateAbbr, politicalState.c.year)

process_count = 0

# Find location code WHERE state and county match
def lookupLocationCode(stateName, countyName):
    state = us.states.lookup(stateName)
    if None == state:
        print("State " + stateName + " not found in US states Python library.")
        sys.exit(-1)
    stateAbbr = state.abbr
    county = countyName.strip().replace(".", "") + ' County'
    # Create Location Key
    locCode = stateAbbr + "-" + string.capwords(county)

    locations = select([locationCodes]).where(locationCodes.c.code == locCode)
    locations_result = conn.execute(locations)
    if 0 == locations_result.rowcount:
        # Location does not exist -> Print an exception
        print("Location " + locCode + " not found in location_codes table.")
        result = None
    elif 1 == locations_result.rowcount:
        # Unique Result -> return location code
        result = locCode
    else:
        # More than one row returned - should not occur -> show error and exit.
        print("Multiple of this location found in file!")
        sys.exit(-1)
    locations_result.close()
    return result


def readCounties(csvfile):
    global process_count
    # Map CSV column names to positions
    irscsvreader = csv.DictReader(csvfile, delimiter=',')

    # Iterate over rows in source CSV file, add locations as needed and store data in SQL tables
    csvrow = irscsvreader.next()
    done = csvrow['Office'] != 'President'
    while not done:
        locCode = lookupLocationCode(csvrow['State'], csvrow['Area'])
        insert_county = politicalCounty.insert()
        inserted_county = conn.execute(insert_county, locCode=locCode,
                                       total_votes=int(csvrow['TotalVotes'].replace(',', '')),
                                       rep_votes=int(csvrow['RepVotes'].replace(',', '')),
                                       dem_votes=int(csvrow['DemVotes'].replace(',', '')),
                                       rep_pct=csvrow['RepVotesTotalPercent'], dem_pct=csvrow['DemVotesTotalPercent'],
                                       year=selected_year)
        process_count += 1
        if not (process_count % 100):
            sys.stdout.write('.')

        csvrow = irscsvreader.next()
        done = csvrow['Office'] != 'President'


def readState(csvfile):
    global process_count
    # Map CSV column names to positions
    irscsvreader = csv.DictReader(csvfile, delimiter=',')

    # Iterate over rows in source CSV file, add locations as needed and store data in SQL tables
    done = False
    while not done:
        csvrow = irscsvreader.next()
        done = csvrow['CensusPopAll'] != 'N/A'
        stateAbbr = us.states.lookup(csvrow['AreaAll']).abbr

        insert_state = politicalState.insert()
        inserted_state = conn.execute(insert_state, stateAbbr=stateAbbr, total_votes=csvrow['TotalVotesAll'],
                                      rep_votes=csvrow['RepVotesAll'], dem_votes=csvrow['DemVotesAll'],
                                      rep_pct=csvrow['RepVotesTotalPercentAll'],
                                      dem_pct=csvrow['DemVotesTotalPercentAll'],
                                      year=selected_year)

        process_count += 1
        if not (process_count % 100):
            sys.stdout.write('.')


dburl = 'mysql://' + username + ':' + password + '@' + hostname + '/popmigration?charset=utf8&use_unicode=0'
engine = create_engine(dburl, pool_recycle=3600)
metadata.create_all(engine)
conn = engine.connect()

ifile = open(csvfilename, 'rb')
end_of_file = False
while not end_of_file:
    pre_read_pos = ifile.tell()
    nextline = ifile.readline()
    if len(nextline) > 0:
        if nextline.startswith("PRESIDENT"):
            selected_year = nextline[10:14]
        elif nextline.startswith(",,,,,"):
            # Do nothing
            sys.stdout.write('_')
        elif nextline.startswith("Office"):
            # Header for County results
            ifile.seek(pre_read_pos, os.SEEK_SET)
            readCounties(ifile)
        elif nextline.startswith("CensusPopAll"):
            # Header for State results
            ifile.seek(pre_read_pos, os.SEEK_SET)
            readState(ifile)
    else:
        end_of_file = True

ifile.close()
