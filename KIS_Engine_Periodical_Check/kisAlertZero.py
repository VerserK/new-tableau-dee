import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload
import os
import tempfile
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__, ContentSettings
import io
import pyodbc
from . import bulkinsert #on cloud
# from bulkinsert import c_bulk_insert
def run():
    #Connect DWH
    server = 'skcdwhprdmi.siamkubota.co.th'
    database =  'KIS Data'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'
    driver = '{ODBC Driver 17 for SQL Server}'
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    table = 'Engine_Periodical_Check'
    conn = pyodbc.connect(dsn)
    cursor = conn.cursor()

    #SetUp Define AzureBlob
    sas_token = "sp=raw&st=2023-05-15T02:31:34Z&se=2023-12-31T10:31:34Z&spr=https&sv=2022-11-02&sr=c&sig=ydPyGiPRFzTCuSriFsFxyI1tTm73YpEzvpHvIfl7OFE%3D"
    account_url = "https://dwhwebstorage.blob.core.windows.net"
    container = "test"
    blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
    container_client = blob_service_client.get_container_client(container=container)

    def upload_csv(local_file_name):
        target_file_name = os.path.basename(local_file_name)
        blob_client = container_client.get_blob_client(target_file_name)
        with open(local_file_name, "rb") as data:
            print("Upload Start")
            content_settings = ContentSettings(content_type='text/plain')
            blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
            print("Upload Done")

    # If modifying these scopes, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/drive']

    creds = None

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

    # Create a unique name for the container
    container_name = 'methee-google-file'

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='ggdriveToken.json')
    creds_path = os.path.join(tempfile.gettempdir(), 'ggdriveToken.json')

    with open(creds_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    creds = Credentials.from_authorized_user_file(creds_path, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            exit
        # Save the credentials for the next run
        with open(creds_path, 'w') as token:
            token.write(creds.to_json())
        with open(creds_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True)

    # create drive api client
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)

    file_id = '1-uc9oOeoFpaCgNO8XGrbkCGyOxRhsuDA'
    file_name = 'KISAlertZero.xlsx'
    file_name_csv = 'KISAlertZero.csv'

    # pylint: disable=maybe-no-member
    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(F'Download {int(status.progress() * 100)}.')

    file.seek(0)

    with open(os.path.join(tempfile.gettempdir(),file_name), 'wb') as f :
        f.write(file.read())
        f.close

    df = pd.read_excel(os.path.join(tempfile.gettempdir(),file_name))
    cursor.execute('TRUNCATE TABLE ' + table)
    conn.commit()
    df.to_csv(os.path.join(tempfile.gettempdir(),file_name_csv),index=False, header=None)
    upload_csv(os.path.join(tempfile.gettempdir(),file_name_csv))
    bulkinsert.c_bulk_insert(file_name_csv, 'skcdwhprdmi.public.bf8966ba22c0.database.windows.net,3342', 'KIS Data', 'skcadminuser', 'DEE@skcdwhtocloud2022prd', 'Engine_Periodical_Check')