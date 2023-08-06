import logging
import azure.functions as func
import pandas as pd
import jwt
import datetime
import uuid
import os, sys
import pickle
import tempfile
import mimetypes
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import io
from io import StringIO

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

connectedAppClientId = os.getenv('connectedAppClientId', None)
connectedAppSecretId = os.getenv('connectedAppSecretId', None)
connectedAppSecretKey = os.getenv('connectedAppSecretKey', None)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    code = req.params.get('code')
    if code:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        SPREADSHEET_ID = '1vZcTEONujHVMA274EtAbRV7Illy6uY4VERHlljTceAU'
        creds = None
  
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net')

        # Create a unique name for the container
        container_name = 'google-file'

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

        RANGE_NAME = 'ConnectedApp!A:C'
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
        values = result.get('values', [])
        df = pd.DataFrame(values[1:], columns=values[0])
        df.columns = ['code','user','url']
        df = df.drop(df[df.code != code].index)
        df.reset_index(drop=True,inplace=True)
        if df.shape[0] > 0:
            url = df['url'][0]
            token = jwt.encode(
                {
                    "iss": connectedAppClientId,
                    "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=10),
                    "jti": str(uuid.uuid4()),
                    "aud": "tableau",
                    "sub": df['user'][0],
                    "scp": ["tableau:views:embed"]
                },
                connectedAppSecretKey,
                algorithm="HS256",
                headers={
                    'kid': connectedAppSecretId,
                    'iss': connectedAppClientId
                }
            )
            script_dir = os.path.dirname(__file__)
            f = open(os.path.join(script_dir, "embed.html"),'r')
            filedata = f.read()
            f.close()

            f_new = filedata.replace("target_url", url)
            f_new = f_new.replace("tab_token", token)

            outfile = os.path.join(tempfile.gettempdir(), 'out.html')

            f = open(outfile,'w')
            f.write(f_new)
            f.close()
            with open(outfile, 'rb') as f:
                mimetype = mimetypes.guess_type(outfile)
                return func.HttpResponse(f.read(), mimetype=mimetype[0])
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400
        )
