import requests
import psycopg2
import csv

host = "localhost"
dbname = "mudano"
user = "postgres"
filepath = "C:\\Users\\Administrator\\Downloads\\Indicator_Data.csv"

def make_request(url):
    """
    Takes the URL and makes a GET request for data.
    Returns the request object if successful.
    """
    req = requests.get(url)

    # If the request status successful, return it to caller
    if req.status_code == 200:
        return req

def convert_json(request):
    """Takes a request object and converts it to json."""
    data = request.json()
    return data

def connect_pg(host, dbname, user):
    """
    Connects to the database using the provided arguments
    Returns a connection object.
    """
    connstring = "host={} dbname={} user={}".format(host, dbname, user)
    conn = psycopg2.connect(connstring)

    return conn

def pg_writedata (conn, data):
    """
    Takes a connection object and a list of values for multiple inserts into the database
    """
    cur = conn.cursor()
    cur.executemany("INSERT INTO countryincome(CountryId, CountryISOCode, CountryName, CapitalCity, RegionId, RegionName, IncomeLevelId, IncomeLevel) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", data)
    conn.commit()

def pg_truncate (conn, tablename):
    """
    Truncates the table provided to the function. Be Careful
    """
    cmd = "TRUNCATE TABLE {}".format(tablename)

    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def csv_loadgep (host, dbname, user, table, filepath):
    conn = connect_pg(host, dbname, user)
    filedata = []

    # Clean down the landing table
    pg_truncate(conn, table)

    with open(filepath, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Don't want the header
        for row in reader:
            filedata.append(row)

    cur = conn.cursor()
    cur.executemany('INSERT INTO GEPData(SeriesName, SeriesCode, CountryName, CountryCode, "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', filedata)
    conn.commit()

def api_getcountry():
    """Wrapper function to call the WorldBank API and load the country data into a database"""

    base_api = 'http://api.worldbank.org/v2/country?format=json' #force the JSON format for now
    req = make_request(base_api)
    data = convert_json(req)
    tabledata = [] #Build a list to hold all the JSON for inserts

    # Get the header data, need to check indicators
    fhead = (data[0])

    # if the default call covers all the data then fine
    if int(fhead['per_page']) >= int(fhead['total']):
        print('default call gets all')

    # otherwise alter the arguments to get the data and remake the request
    else:
        print('default call doesnt get all')
        perpage = '&per_page=' + str(fhead['total'])
        api = base_api + perpage
        req = make_request(api)
        data = convert_json(req)

    for row in data[1]:
        rowdata = []
        top = row
        region = row['region']
        inclevel = row['incomeLevel']

        # Pick up the data we want to store
        rowdata.append(top['id'])
        rowdata.append(top['iso2Code'])
        rowdata.append(top['name'])
        rowdata.append(top['capitalCity'])
        rowdata.append(region['id'])
        rowdata.append(region['value'])
        rowdata.append(inclevel['id'])
        rowdata.append(inclevel['value'])

        #Append the rowdata list to the table data list
        tabledata.append(rowdata)

    conn = connect_pg(host, dbname, user)
    pg_truncate(conn, 'countryincome')
    pg_writedata(conn, tabledata)

    conn.close()



api_getcountry() #Make the API call and get the Country data
csv_loadgep (host, dbname, user, 'gepdata', filepath) #Load the csv from the filepath variable. Change variable if not stored in Downloads