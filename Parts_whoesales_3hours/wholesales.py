from ftplib import FTP
import pandas as pd
import numpy as np
import pyodbc
import os
import datetime
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
from . import bulkinsert #on cloud
# from bulkinsert import c_bulk_insert
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import logging
import tempfile
import pickle

#SetUp Define AzureBlob
sas_token = "sp=racwdli&st=2023-08-03T01:30:28Z&se=2030-08-03T09:30:28Z&spr=https&sv=2022-11-02&sr=c&sig=C8fvjhxkPCyHiThiNYlkfhz1w%2FVdizP7P1EOYBEEOBY%3D"
account_url = "https://dwhwebstorage.blob.core.windows.net"
container = "test"
blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client = blob_service_client.get_container_client(container=container)

#Connect DWH
def connect_db(sql_server_nm, db_nm, username, password):
        # Connect to the server and database with Windows authentication.
        conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + sql_server_nm + ';DATABASE=' + db_nm + ';UID='+ username +';PWD='+ password
        conn = pyodbc.connect(conn_string)
        return conn

def upload_csv(local_file_name):
    target_file_name = os.path.basename(local_file_name)
    blob_client = container_client.get_blob_client(target_file_name)
    with open(local_file_name, "rb") as data:
        print("Upload Start")
        content_settings = ContentSettings(content_type='text/plain')
        blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
        print("Upload Done")

def stamp_log(table,flag=False):
    if flag:
        status = 'Success'
    else:
        status = 'Fail'
    timestamp = str(datetime.datetime.today())
    SPREADSHEET_ID='1Ce5A91xYxhCABtwSFinfy2sIJmh_aeoKlFXvSz1ypH4'
    RANGE_NAME='Sheet1'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive']

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

    # Create a unique name for the container
    container_name = 'methee-google-file'

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='sheet-token.pickle')
    creds_path = os.path.join(tempfile.gettempdir(), 'sheet-token.pickle')

    with open(creds_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    with open(creds_path, "rb") as token:
        creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            exit
        # Save the credentials for the next run
        with open(creds_path, 'wb') as token:
            pickle.dump(creds, token)
        with open(creds_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    service = build('sheets', 'v4', credentials=creds)

    service = service.spreadsheets()
   
    sheet = service.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE_NAME).execute()
    values = sheet.get('values', [])
    logs = pd.DataFrame(values[1:],columns=values[0])
    logs.index = logs['Table']
    logs.loc[table,['Table']] = table
    logs.loc[table,['Log timestamp']] = timestamp
    logs.loc[table,['Status']] = status
    logs['last success'] = np.where((logs['Table'].eq(table))&(status == 'Success'),timestamp,logs['last success'])
    
    logs = [logs.columns.to_list()] + logs.values.tolist()
    
    response_update = service.values().update(spreadsheetId=SPREADSHEET_ID,
                        range=RANGE_NAME,valueInputOption='USER_ENTERED',
                        body=dict(majorDimension='ROWS',
                        values=logs)).execute()
    return logs

def run():
    #Tempfile Path
    path = tempfile.gettempdir()

    # Print out the files
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob.name)
        if blob.name == 'ws_data.csv':
            logging.info("Downloading..." + blob.name)
            with open(os.path.join(path,blob.name), mode='wb') as sample_blob:
                download_stream = blob_client.download_blob()
                sample_blob.write(download_stream.readall())
            logging.info('Finished Download File ' + blob.name)

            #Prep Data
            df = pd.read_csv(os.path.join(path,blob.name))
            col_name = ['SaleOrder','Orderitem','custpo','OrderDate','ReqDate','Del1stDate','PricingDate','SOType','itemcat','SOrg','DistCh','division','sloc','plant','soldto','shipto','payer','PartNo','qty','idreason','reason_desc','unit','listprice','total_listprice','netvalue','total_netvalue','Currency','Changed date (SO item)']
            df.columns = col_name
            df['OrderDate'] = pd.to_datetime(df['OrderDate'],format='%d.%m.%Y')
            df['ReqDate'] = pd.to_datetime(df['ReqDate'],format='%d.%m.%Y')
            df['Del1stDate'] = pd.to_datetime(df['Del1stDate'],format='%d.%m.%Y')
            df['PricingDate'] = pd.to_datetime(df['PricingDate'],format='%d.%m.%Y')
            df['Changed date (SO item)'] = pd.to_datetime(df['Changed date (SO item)'], errors='coerce',format='%d.%m.%Y')
            logging.info('Datetime Start')
            cursor = connect_db('skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'Parts', 'skcadminuser', 'DEE@skcdwhtocloud2022prd').cursor()
            qry = 'DELETE FROM [Parts].[dbo].[wholesale] WHERE OrderDate >= ? '
            df = df['OrderDate'].drop_duplicates()
            cursor.execute(qry,df.min())
            cursor.commit()
            logging.info('Datetime End:')

        if blob.name == 'wschange_data.csv':
            logging.info("Downloading..." + blob.name)
            with open(os.path.join(path,blob.name), mode='wb') as sample_blob:
                download_stream = blob_client.download_blob()
                sample_blob.write(download_stream.readall())

            #Prep Data
            df = pd.read_csv(os.path.join(path,blob.name))
            col_name = ['SaleOrder','Orderitem','custpo','OrderDate','ReqDate','Del1stDate','PricingDate','SOType','itemcat','SOrg','DistCh','division','sloc','plant','soldto','shipto','payer','PartNo','qty','idreason','reason_desc','unit','listprice','total_listprice','netvalue','total_netvalue','Currency','Changed date (SO item)']
            df.columns = col_name
            df['PartNo'] = df['PartNo'].astype(str).apply(lambda x: x if (len(x) <= 11) else f"{x[:11]}-{x[11:]}")
            df['OrderDate'] = pd.to_datetime(df['OrderDate'],format='%d.%m.%Y')
            df['ReqDate'] = pd.to_datetime(df['ReqDate'],format='%d.%m.%Y')
            df['Del1stDate'] = pd.to_datetime(df['Del1stDate'],format='%d.%m.%Y')
            df['PricingDate'] = pd.to_datetime(df['PricingDate'], format='%d.%m.%Y', errors = 'coerce')
            df["PricingDate"].fillna("9999-12-31", inplace = True)
            df['Changed date (SO item)'] = pd.to_datetime(df['Changed date (SO item)'], errors='coerce',format='%d.%m.%Y')
            out = df[(df['SaleOrder'] == df['SaleOrder'])& (df['Orderitem'] == df['Orderitem']) & (df['Changed date (SO item)'] == df['Changed date (SO item)'].max())]
            out_final = pd.concat([df, out]).drop_duplicates(subset=['SaleOrder', 'Orderitem'], keep='last')
            out_final = out_final.fillna(0)

            dfl = []
            startDate = datetime.datetime.today().strftime("%Y-%m-%d, %H:%M:%S")
            chunksize = 100000
            chunksizeNum = 100000

            logging.info('Start Query SQL' + startDate)
            if os.path.exists(path + '\dfTest.csv') == True:
                os.remove(os.path.join(path,'dfTest.csv'))

            for chunk in pd.read_sql_query(sql="SELECT * FROM [Parts].[dbo].[wholesale]", con=connect_db('skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'Parts', 'skcadminuser', 'DEE@skcdwhtocloud2022prd'), chunksize=chunksize):
                # Start Appending Data Chunks from SQL Result set into List
                dfl.append(chunk)
                logging.info('Count Chunk ' + str(chunksizeNum))
                chunksizeNum += chunksize
            dfTest = pd.concat(dfl, ignore_index=True)
            dfTest.to_csv(os.path.join(path,'dfTest.csv'), mode='a', index=False, header=None)
            #Start appending data from list to dataframe
            dfTest = pd.read_csv(os.path.join(path,'dfTest.csv'))
            chunksize = 10 ** 6
            with pd.read_csv(os.path.join(path,'dfTest.csv'), chunksize=chunksize) as reader:
                for chunk in reader:
                    print(chunk)
            logging.info('Read_sql time for table 1')

            col_name = ['SaleOrder','Orderitem','custpo','OrderDate','ReqDate','Del1stDate','PricingDate','SOType','itemcat','SOrg','DistCh','division','sloc','plant','soldto','shipto','payer','PartNo','qty','idreason','reason_desc','unit','listprice','total_listprice','netvalue','total_netvalue','Currency']
            dfTest.columns = col_name

            dfTest['SaleOrder'] = dfTest['SaleOrder'].astype(int)
            dfTest['Orderitem'] = dfTest['Orderitem'].astype(int)

            dfPrep = dfTest.merge(out_final, on=['SaleOrder','Orderitem'], how='left')
            dfPrep1 = [x for x in dfPrep if x.endswith('_y')]
            dfPrep.drop(dfPrep1, axis=1, inplace=True)
            for col in dfPrep:
                if col.endswith('_x'):
                    dfPrep.rename(columns={col:col.rstrip('_x')}, inplace=True)

            logging.info('Finished Merge Dataframe')
            dfPrep['Currency'] = dfPrep['Currency'].str.strip('"')
            dfPrep = dfPrep[dfPrep.columns[:-1]]

            df = pd.read_csv(os.path.join(path,'ws_data.csv'))
            col_name = ['SaleOrder','Orderitem','custpo','OrderDate','ReqDate','Del1stDate','PricingDate','SOType','itemcat','SOrg','DistCh','division','sloc','plant','soldto','shipto','payer','PartNo','qty','idreason','reason_desc','unit','listprice','total_listprice','netvalue','total_netvalue','Currency','Changed date (SO item)']
            df.columns = col_name
            df['PartNo'] = df['PartNo'].astype(str).apply(lambda x: x if (len(x) <= 11) else f"{x[:11]}-{x[11:]}")
            df['OrderDate'] = pd.to_datetime(df['OrderDate'],format='%d.%m.%Y')
            df['ReqDate'] = pd.to_datetime(df['ReqDate'],format='%d.%m.%Y')
            df['Del1stDate'] = pd.to_datetime(df['Del1stDate'],format='%d.%m.%Y')
            df['PricingDate'] = pd.to_datetime(df['PricingDate'], format='%d.%m.%Y', errors = 'coerce')
            df["PricingDate"].fillna("9999-12-31", inplace = True)
            df['Changed date (SO item)'] = pd.to_datetime(df['Changed date (SO item)'], errors='coerce',format='%d.%m.%Y')
            start_ts = datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
            logging.info('Datetime Upload Start :', start_ts)
            df2 = df[df.columns[:-1]]
            
            df3 = dfPrep.merge(df2, on=['SaleOrder','Orderitem'], how='right')
            df31 = [x for x in df3 if x.endswith('_x')]
            df3.drop(df31, axis=1, inplace=True)
            for col in df3:
                if col.endswith('_y'):
                    df3.rename(columns={col:col.rstrip('_y')}, inplace=True)
            col_name = ['SaleOrder','Orderitem','custpo','OrderDate','ReqDate','Del1stDate','PricingDate','SOType','itemcat','SOrg','DistCh','division','sloc','plant','soldto','shipto','payer','PartNo','qty','idreason','reason_desc','unit','listprice','total_listprice','netvalue','total_netvalue','Currency']
            df3.columns = col_name
            result = pd.concat([dfPrep,df3], ignore_index=True).drop_duplicates(keep='last', subset=['SaleOrder','Orderitem'])

            logging.info('Finished Merge Dataframe')
            
            result.to_csv(os.path.join(path,'halfday','ws_data_final.csv'), index=False, header=None)
            mydb = connect_db('skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'Parts', 'skcadminuser', 'DEE@skcdwhtocloud2022prd')
            cursor = mydb.cursor()
            qry = 'TRUNCATE TABLE [Parts].[dbo].[wholesale]'
            cursor.execute(qry)
            cursor.commit()
            logging.info('TRUNCATE Complete')
            upload_csv(os.path.join(path,'halfday', "ws_data_final.csv"))
            flag = bulkinsert.c_bulk_insert('ws_data_final.csv', 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'Parts', 'skcadminuser', 'DEE@skcdwhtocloud2022prd', 'wholesale')
            stamp_log('wholesales',flag)