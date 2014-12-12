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
                       Column("stateAbbr", String(2), nullable=False),
                       Column("total_votes", Integer, nullable=False),
                       Column("rep_votes", Integer, nullable=False),
                       Column("dem_votes", Integer, nullable=False),
                       Column("rep_pct", Numeric(5, 2), nullable=False),
                       Column("dem_pct", Numeric(5, 2), nullable=False),
                       Column("year", Integer, nullable=False),
)

Index("polstate_index", politicalState.c.stateAbbr, politicalState.c.year)

process_count = 0

class MultiCsvReader(csv.DictReader):
    def __init__(self, f, fieldnames=None, restkey=None, restval=None,
                 dialect="excel", *args, **kwds):
        csv.DictReader.__init__(self, f, fieldnames, restkey, restval,
                 dialect, *args, **kwds)

    def readline(self):
        return self.reader.next()


# Find location code WHERE state and county match
def lookupLocationCode(stateName, countyName, areaType):
    state = us.states.lookup(stateName)
    if None == state:
        print("State " + stateName + " not found in US states Python library.")
        sys.exit(-1)
    stateAbbr = state.abbr
    county = countyName.strip().replace(".", "") + ' ' + areaType
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


def readCounties(rdr):
    global process_count
    # Map CSV column names to positions

    # Iterate over rows in source CSV file, add locations as needed and store data in SQL tables
    csvrow = rdr.next()
    done = csvrow['Office'] != 'President'
    while not done:
        locCode = lookupLocationCode(csvrow['State'], csvrow['Area'], csvrow['AreaType'])
        for i in range (0, 4):
            insert_county = politicalCounty.insert()
            inserted_county = conn.execute(insert_county, locCode=locCode,
                                       total_votes=int(csvrow['TotalVotes'].replace(',', '')),
                                       rep_votes=int(csvrow['RepVotes'].replace(',', '')),
                                       dem_votes=int(csvrow['DemVotes'].replace(',', '')),
                                       rep_pct=csvrow['RepVotesTotalPercent'], dem_pct=csvrow['DemVotesTotalPercent'],
                                       year=int(selected_year)+i)
        process_count += 1
        if not (process_count % 100):
            sys.stdout.write('.')

        csvrow = rdr.next()
        done = csvrow['Office'] != 'President'


def readState(rdr):
    global process_count
    global end_of_file
    # Map CSV column names to positions

    # Iterate over rows in source CSV file, add locations as needed and store data in SQL tables
    csvrow = rdr.next()
    done = csvrow['CensusPopAll'] != 'N/A'
    while not done:
        stateAbbr = us.states.lookup(csvrow['AreaAll']).abbr
        for i in range (0, 4):
            insert_state = politicalState.insert()
            inserted_state = conn.execute(insert_state, stateAbbr=stateAbbr,
                                      total_votes=int(csvrow['TotalVotesAll'].replace(',', '')),
                                      rep_votes=int(csvrow['RepVotesAll'].replace(',', '')),
                                      dem_votes=int(csvrow['DemVotesAll'].replace(',', '')),
                                      rep_pct=csvrow['RepVotesTotalPercentAll'],
                                      dem_pct=csvrow['DemVotesTotalPercentAll'],
                                      year=int(selected_year)+i)

        process_count += 1
        if not (process_count % 100):
            sys.stdout.write('.')

        try:
            csvrow = rdr.next()
            done = csvrow['CensusPopAll'] != 'N/A'
        except StopIteration:
            done = True
            end_of_file = True


dburl = 'mysql://' + username + ':' + password + '@' + hostname + '/popmigration?charset=utf8&use_unicode=0'
engine = create_engine(dburl, pool_recycle=3600)
metadata.create_all(engine)
conn = engine.connect()

ifile = open(csvfilename, 'rb')
end_of_file = False
gen_iter = MultiCsvReader(ifile, delimiter=',')
gen_csv_row = gen_iter.readline()
col1 = gen_csv_row[0]
while gen_csv_row and not end_of_file:
    if col1.startswith("PRESIDENT"):
        selected_year = col1[10:14]
    elif 0 == len(col1):
        # Do nothing
        sys.stdout.write('_')
    elif col1.startswith("Office"):
        # Header for County results
        gen_iter.fieldnames = gen_csv_row
        readCounties(gen_iter)
    elif col1.startswith("CensusPopAll"):
        # Header for State results
        gen_iter.fieldnames = gen_csv_row
        readState(gen_iter)
    if not end_of_file:
        gen_csv_row = gen_iter.readline()
        col1 = gen_csv_row[0]

ifile.close()
