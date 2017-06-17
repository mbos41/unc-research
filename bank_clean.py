import pyodbc, csv
# pyodbc documentation available at: https://github.com/mkleehammer/pyodbc/wiki


# Create connection to access file
def open_mdb(month, mon, day, year):
    drv = 'Microsoft Access Driver (*.mdb, *.accdb)'
    if day < 10:
        mdb = '{}/D0{}0{}{}.mdb;'.format(month, day, mon, year)
    else:
        mdb = '{}/D{}0{}{}.mdb;'.format(month, day, mon, year)
    #print(mdb)
    cnxn = pyodbc.connect('DRIVER={};DBQ={}'.format(drv, mdb))
    return cnxn.cursor()



# Read in table from access file and write to csv
def read_write(table_type, date):
    SQL = 'SELECT * FROM {};'.format(table_type) 
    rows = crsr.execute(SQL).fetchall()
        


# Define months, days and table types to iterate through
months = ['January2008', 'February2008', 'March2008']
days = [31, 29, 31]
tables = ['agent_events', 'agent_profile', 'agent_records', 'agent_shifts',
          'calls', 'cust_subcalls', 'event_details', 'server_subcalls']

header = col_dict(tables)

# Main loop to iterate through all tables and write to csv
for i, month in enumerate(months):
    for day in range(1,days[i]+1):
        print(month, "Day:", day)
        crsr = open_mdb(month, i+1, day)
        if day < 10:
            date_string = 'D0{}0{}2008'.format(day,i+1)
        else:
            date_string = 'D{}0{}2008'.format(day,i+1)
        for table in tables:
            read_write(table, date_string)

crsr.close()
#cnxn.close()
