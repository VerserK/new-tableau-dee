import pyodbc

class c_bulk_insert:
    def __init__(self, csv_file_nm, sql_server_nm, db_nm, username, password, db_table_nm):
        # Connect to the database, perform the insert, and update the log table.
        conn = self.connect_db(sql_server_nm, db_nm, username, password)
        self.insert_data(conn, csv_file_nm, db_table_nm)
        conn.close
    def connect_db(self, sql_server_nm, db_nm, username, password):
        # Connect to the server and database with Windows authentication.
        conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server_nm + ';DATABASE=' + db_nm + ';UID='+ username +';PWD='+ password
        conn = pyodbc.connect(conn_string)
        return conn
    def insert_data(self, conn, csv_file_nm, db_table_nm):
        cursor = conn.cursor()
        # Insert the data from the CSV file into the database table.
        # Assemble the BULK INSERT query. Be sure to skip the header row by specifying FIRSTROW = 2.
        qry = "BULK INSERT " + db_table_nm + " FROM '" + csv_file_nm + "' WITH (DATA_SOURCE = 'KISData', FIELDTERMINATOR =',', FORMAT = 'CSV', DATAFILETYPE ='char', ROWTERMINATOR = '0x0a', CODEPAGE = '65001')"
        # Execute the query
        # cursor = conn.cursor()
        # if encoding == 'cp874':
        #     cursor.execute(qry % '874')
        # else: #0x0a
        #     cursor.execute(qry % '65001')
        try:
            cursor.execute(qry)
            conn.commit()
            print("Bulk Insert Complete")
            conn.close
            return True
        except Exception as e:
            conn.rollback()
            conn.close()        
            print(str(e))
            return False