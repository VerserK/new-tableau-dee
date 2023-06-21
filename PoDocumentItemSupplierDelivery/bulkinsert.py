import logging
import sqlalchemy as sa
from sqlalchemy import text as sa_text
import urllib

class c_bulk_insert:
    def __init__(self, csv_file_nm, sql_server_nm, db_nm, username, password, db_table_nm):
        # Connect to the database, perform the insert, and update the log table.
        conn = self.connect_db(sql_server_nm, db_nm, username, password)
        self.insert_data(conn, csv_file_nm, db_table_nm)
        conn.close
    def connect_db(self, sql_server_nm, db_nm, username, password):
        # Connect to the server and database with Windows authentication.
        conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server_nm + ';DATABASE=' + db_nm + ';UID='+ username +';PWD='+ password
        params = urllib.parse.quote_plus(conn_string)
        engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
        connection = engine.connect()
        # conn = pyodbc.connect(conn_string)
        return connection
    def insert_data(self, connection, csv_file_nm, db_table_nm):
        
        # Insert the data from the CSV file into the database table.
        # Assemble the BULK INSERT query. Be sure to skip the header row by specifying FIRSTROW = 2.
        qry = "BULK INSERT " + db_table_nm + " FROM '" + csv_file_nm + "' WITH (DATA_SOURCE = 'eprocurement1', FIELDTERMINATOR =',', FORMAT = 'CSV', DATAFILETYPE ='char', ROWTERMINATOR = '0x0a', CODEPAGE = '65001')"
        # Execute the query
        # cursor = conn.cursor()
        # if encoding == 'cp874':
        #     cursor.execute(qry % '874')
        # else: #0x0a
        #     cursor.execute(qry % '65001')
        connection.execute(sa_text(qry).execution_options(autocommit=True))
        logging.info("Bulk Insert Complete")
        # cursor.close