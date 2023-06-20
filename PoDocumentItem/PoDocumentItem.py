import pandas as pd
import os
import tempfile
import sqlalchemy as sa
from sqlalchemy.sql import text as sa_text
import urllib
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

def run():
    #configure sql server
    server = 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342'
    database =  'E_Procurement'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'PoDocumentItem'
    params = urllib.parse.quote_plus(dsn)
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)

    #PATH
    tempFilePath = tempfile.gettempdir()

    nameFile = table + '.csv'
    qry = 'SELECT * FROM [172.29.196.79].[SKCeProcurement].[dbo].' + table
    df = pd.read_sql_query(qry, con=engine)
    df['PRItemId'] = df['PRItemId'].fillna(993344)
    df['PRItemId'] = df['PRItemId'].astype(int)
    df['PRItemId'] = df['PRItemId'].replace(993344,None)
    df['PRItemNo'] = df['PRItemNo'].fillna(993344)
    df['PRItemNo'] = df['PRItemNo'].astype(int)
    df['PRItemNo'] = df['PRItemNo'].replace(993344,None)
    df['InfoRecordUpdate'] = df['InfoRecordUpdate'].replace(True,1)
    df['InfoRecordUpdate'] = df['InfoRecordUpdate'].replace(False,0)
    df['UnlimitedOverDeliveryAllowed'] = df['UnlimitedOverDeliveryAllowed'].replace(True,1)
    df['UnlimitedOverDeliveryAllowed'] = df['UnlimitedOverDeliveryAllowed'].replace(False,0)
    df['GRStatus'] = df['GRStatus'].replace(True,1)
    df['GRStatus'] = df['GRStatus'].replace(False,0)
    df['GRNonValuated'] = df['GRNonValuated'].replace(True,1)
    df['GRNonValuated'] = df['GRNonValuated'].replace(False,0)
    df['DeliveryCompleted'] = df['DeliveryCompleted'].replace(True,1)
    df['DeliveryCompleted'] = df['DeliveryCompleted'].replace(False,0)
    df['InvoiceReceipt'] = df['InvoiceReceipt'].replace(True,1)
    df['InvoiceReceipt'] = df['InvoiceReceipt'].replace(False,0)
    df['FinalInvoice'] = df['FinalInvoice'].replace(True,1)
    df['FinalInvoice'] = df['FinalInvoice'].replace(False,0)
    df['GRBasedInvoice'] = df['GRBasedInvoice'].replace(True,1)
    df['GRBasedInvoice'] = df['GRBasedInvoice'].replace(False,0)
    df['EvaluatedReceiptSettlement'] = df['EvaluatedReceiptSettlement'].replace(True,1)
    df['EvaluatedReceiptSettlement'] = df['EvaluatedReceiptSettlement'].replace(False,0)
    print(df.dtypes)
    engine.execute(sa_text(('TRUNCATE TABLE ') + table).execution_options(autocommit=True))
    df.to_csv(os.path.join(tempFilePath,nameFile), index=False, encoding='utf-8', header=None)
    upload_csv(os.path.join(tempFilePath, nameFile))
    bulkinsert.c_bulk_insert(nameFile, 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'E_Procurement', 'skcadminuser', 'DEE@skcdwhtocloud2022prd', table)