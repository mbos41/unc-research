import pyodbc, csv
# pyodbc documentation available at: https://github.com/mkleehammer/pyodbc/wiki


# Create connection to access file
def open_mdb(month, mon, day):
    drv = 'Microsoft Access Driver (*.mdb, *.accdb)'
    if day < 10:
        mdb = 'Y:/{}/D0{}0{}2008.mdb;'.format(month, day, mon)
    else:
        mdb = 'Y:/{}/D{}0{}2008.mdb;'.format(month, day, mon)
    #print(mdb)
    cnxn = pyodbc.connect('DRIVER={};DBQ={}'.format(drv, mdb))
    return cnxn.cursor()

# Read in table from access file and write to csv
def read_write(table_type, date):
    SQL = 'SELECT * FROM {};'.format(table_type) 
    rows = crsr.execute(SQL).fetchall()

    # Add column names to beginning
    rows.insert(0, header[table_type])

    # Write data to individual date file
    with open('Z:/test/{}_{}.csv'.format(date, table_type), 'w', newline = '') as fou:
        csv_writer = csv.writer(fou) 
        csv_writer.writerows(rows)
        
# Extract column names for each table type and save in dictionary
def col_dict(tables):    
    header_dict = {}
    crsr = open_mdb('January2008', 1, 1)
    # Output list of column names
    for table_type in tables:
        names = []
        for col in crsr.columns(table=table_type):
            names.append(col.column_name)    
        header_dict[table_type] = names
    return header_dict 

# Define months, days and table types to iterate through
months = ['January2008']
days = [31]
tables = ['agent_events', 'agent_profile', 'agent_records', 'agent_shifts',
          'calls', 'cust_subcalls']

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


