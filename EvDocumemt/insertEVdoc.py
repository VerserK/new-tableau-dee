import pyodbc
import pandas as pd
import os
import tempfile
from . import bulkinsert #on cloud
# from bulkinsert import c_bulk_insert
from azure.storage.blob import BlobServiceClient, ContentSettings
import logging

#SetUp Define AzureBlob
sas_token = "sp=racwdli&st=2023-06-19T04:10:35Z&se=2030-12-31T12:10:35Z&spr=https&sv=2022-11-02&sr=c&sig=x8r7JytvrciGWodAcFtpEKYFcavz16Wbdhb6%2BuLYujk%3D"
account_url = "https://deestoragefunction.blob.core.windows.net"
container = "eprocurement"
blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client = blob_service_client.get_container_client(container=container)

def upload_csv(local_file_name):
    target_file_name = os.path.basename(local_file_name)
    blob_client = container_client.get_blob_client(target_file_name)
    with open(local_file_name, "rb") as data:
        logging.info("Upload Start")
        content_settings = ContentSettings(content_type='text/plain')
        blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
        logging.info("Upload Done")

#configure sql server
server = 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342'
database =  'E_Procurement'
username = 'skcadminuser'
password = 'DEE@skcdwhtocloud2022prd'
driver = '{ODBC Driver 17 for SQL Server}'
dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
mydb = pyodbc.connect(dsn)
cursor = mydb.cursor()

#PATH
tempFilePath = tempfile.gettempdir()
def run():
    qry = 'SELECT * FROM [172.29.196.79].[SKCeProcurement].[dbo].[EvDocument]'
    df = pd.read_sql_query(qry, con=mydb)
    df.to_csv(os.path.join(tempFilePath,'EvDocument.csv'),index=False, header=None)
    upload_csv(os.path.join(tempFilePath, "EvDocument.csv"))
    bulkinsert.c_bulk_insert('EvDocument.csv', 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'E_Procurement', 'skcadminuser', 'DEE@skcdwhtocloud2022prd', 'EvDocument')