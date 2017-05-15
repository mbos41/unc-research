import pyodbc, csv


def open_mdb(month, mon, day):
    drv = 'Microsoft Access Driver (*.mdb, *.accdb)'
    if day < 10:
        mdb = '{}/D0{}0{}2008.mdb;'.format(month, day, mon)
    else:
        mdb = '{}/D{}0{}2008.mdb;'.format(month, day, mon)
    #print(mdb)
    cnxn = pyodbc.connect('DRIVER={};DBQ={}'.format(drv, mdb))
    return cnxn.cursor()

def create_masters(col_names, table_type):
    with open('data/{}/Master_{}.csv'.format(table_type, table_type), 'w', newline = '') as fou:
        csv_writer = csv.writer(fou) 
        csv_writer.writerows([col_names])

def read_write(table_type, date):
    SQL = 'SELECT * FROM {};'.format(table_type) 
    rows = crsr.execute(SQL).fetchall()

    if date == 'D01012008':
        create_masters(header[table_type], table_type)

    # Append data to master file
    with open('data/{}/master_{}.csv'.format(table_type, table_type), 'a', newline = '') as fou:
        csv_writer = csv.writer(fou) 
        csv_writer.writerows(rows)

    # Add column names to beginning
    rows.insert(0, header[table_type])

    # Write data to individual date file
    with open('data/{}/{}_{}.csv'.format(table_type, date, table_type), 'w', newline = '') as fou:
        csv_writer = csv.writer(fou) 
        csv_writer.writerows(rows)
        

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

months = ['January2008', 'February2008', 'March2008']
days = [31, 29, 31]
#months = ['January2008']
#days = [3]

tables = ['agent_events', 'agent_profile', 'agent_records', 'agent_shifts',
          'calls', 'cust_subcalls', 'event_details', 'server_subcalls']

header = col_dict(tables)

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
