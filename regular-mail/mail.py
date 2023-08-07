from __future__ import print_function

import pickle
import pandas as pd
import os.path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from datetime import datetime,timedelta
from googleapiclient.errors import HttpError
import base64, requests
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import mimetypes
import os,io,shutil
import tempfile
from croniter import croniter
from os import listdir
import io
from io import StringIO

from apiclient import errors

import os
from azure.storage.blob import BlobServiceClient, __version__

### DOWNLOAD DASHBOARD EXCEL ###
def tableau_get_xls(view_id,fName,fValue,dbName):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "MailNotifyExcel",
        		"personalAccessTokenSecret": "Jt0/L4XsT0WA21RNm+1EOw==:HcQK0cH8iaXqpx6GYoSFfpqgtVbjOKNo",
        		"site": {
        			"contentUrl": "skctableau"
        		}
                }
        }
    res = requests.post(urlHis, headers=headers, json = payload)
    response =  res.json()
    token = response['credentials']['token']
    site_id = response['credentials']['site']['id']
    headers = {"Content-Type": "application/json",
           "Accept":"application/json",
           "X-Tableau-Auth": token}
    url = server +  '/sites/'+site_id+'/views/'+view_id+'/crosstab/excel?maxAge=1&vf_{0}={1}'.format(fName,fValue)
    #res = requests.get(url, headers=headers, json = {})
    res = requests.get(url, headers=headers, allow_redirects=True)
    filename = dbName+'-'+fValue+'.xlsx'
    creds_path = os.path.join(tempfile.gettempdir(), filename)
    file = open(creds_path, "wb")
    file.write(res.content)
    file.close()
    return filename

### DOWNLOAD DASHBOARD IMAGE ###
def tableau_get_img(view_id,fName,fValue,dbName):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "MailNotifyExcel",
        		"personalAccessTokenSecret": "Jt0/L4XsT0WA21RNm+1EOw==:HcQK0cH8iaXqpx6GYoSFfpqgtVbjOKNo",
        		"site": {
        			"contentUrl": "skctableau"
        		}
                }
        }
    res = requests.post(urlHis, headers=headers, json = payload)
    response =  res.json()
    token = response['credentials']['token']
    site_id = response['credentials']['site']['id']
    headers = {"Content-Type": "application/json",
           "Accept":"application/json",
           "X-Tableau-Auth": token}
    url = server +  '/sites/'+site_id+'/views/'+view_id+'/image?maxAge=1&resolution=high&vf_{0}={1}'.format(fName,fValue)
    res = requests.get(url, headers=headers, json = {})
    filename = dbName+'.jpeg'
    creds_path = os.path.join(tempfile.gettempdir(), filename)
    file = open(creds_path, "wb")
    file.write(res.content)
    file.close()
    return filename

### DOWNLOAD FILE FROM GOOGLE DRIVE FOLDER ###
def gfileGETfolder(folderid):
  SCOPES = ['https://www.googleapis.com/auth/drive']
  creds = None
  # Create the BlobServiceClient object which will be used to create a container client
  blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net')

  # Create a unique name for the container
  container_name = 'google-file'

  # Create a blob client using the local file name as the name for the blob
  blob_client = blob_service_client.get_blob_client(container=container_name, blob='drive-token.pickle')
  creds_path = os.path.join(tempfile.gettempdir(), 'drive-token.pickle')

  with open(creds_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

  with open(creds_path, "rb") as token:
    creds = pickle.load(token)
  #creds = Credentials.from_authorized_user_file(creds_path, SCOPES)
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
  service = build('drive', 'v3', credentials=creds)

  page_token = None
  file_list = list()
  while True:
      response = service.files().list(q="'" + folderid + "' in parents",
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token).execute()
      for file in response.get('files', []):
          # Process change
          print('Found file: %s (%s)' % (file.get('name'), file.get('id')))
          request1 = service.files().get_media(fileId=file.get('id'))
          fh = io.BytesIO()
          downloader = MediaIoBaseDownload(fh, request1)
          done = False
          while done is False:
              status, done = downloader.next_chunk()
          else:
              fh.seek(0)
              path = os.path.join(tempfile.gettempdir(), file.get('name'))
              with open(path, 'wb') as f:
                  shutil.copyfileobj(fh, f, length=131072)
                  f.close()
              file_list.append(file.get('name'))
      page_token = response.get('nextPageToken', None)
      if page_token is None:
          break
  return file_list

### DOWNLOAD FILE FROM GOOGLE DRIVE ###
def gfileGET(fileid):
  SCOPES = ['https://www.googleapis.com/auth/drive']
  creds = None
  # Create the BlobServiceClient object which will be used to create a container client
  blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=d710rgsi01diag;AccountKey=nr/2Yn9nN9bWr0GNNSiNvBbN91MfYpkcIK0+9xcrYMdrFttcEAqV4kBBGGd8ehk+BRZ0gfe0iOTeoYVlRNbXOw==;EndpointSuffix=core.windows.net')

  # Create a unique name for the container
  container_name = 'methee-google-file'

  # Create a blob client using the local file name as the name for the blob
  blob_client = blob_service_client.get_blob_client(container=container_name, blob='drive-token.pickle')
  creds_path = os.path.join(tempfile.gettempdir(), 'drive-token.pickle')

  with open(creds_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

  with open(creds_path, "rb") as token:
    creds = pickle.load(token)
  #creds = Credentials.from_authorized_user_file(creds_path, SCOPES)
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
  service = build('drive', 'v3', credentials=creds)
  request = service.files().get(fileId=fileid).execute()
  request1 = service.files().get_media(fileId=fileid)
  fh = io.BytesIO()
  downloader = MediaIoBaseDownload(fh, request1)
  done = False
  while done is False:
      status, done = downloader.next_chunk()
  else:
      fh.seek(0)
      path = os.path.join(tempfile.gettempdir(), request['name'])
      with open(path, 'wb') as f:
          shutil.copyfileobj(fh, f, length=131072)
          f.close()
      return request['name']

### CREATE MAIL WITH ATTACHMENT ###
def create_message_with_attachment(sender, to, cc, bcc, subject, message_text, file_list,iwidth):
  core_message = MIMEMultipart('mixed')
  core_message['to'] = to
  if cc != '':
      core_message['cc'] = cc
  if bcc != '':
      core_message['bcc'] = bcc
  core_message['from'] = sender
  core_message['subject'] = subject

  core_message.preamble = 'This is a multi-part message in MIME format.'
  main_message = MIMEMultipart('related')
  core_message.attach(main_message)
  message = MIMEMultipart('alternative')
  main_message.attach(message)

  txt_list = message_text.split('\n')
  html_text = ''
  for text in txt_list:
    if text.startswith('(im='):
      new_text = text.split('=')[1]
      new_text = new_text[:-1]
      html_text = html_text + '<div><img src="cid:' + new_text + '" width=' + iwidth + '><br></div>'
    else:
      html_text = html_text + '<div><p>' + text + '</p></div>'

  for file in file_list:
    content_type, encoding = mimetypes.guess_type(file)
    creds_path = os.path.join(tempfile.gettempdir(), file)
    if content_type is None or encoding is not None:
      content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    if main_type == 'text':
      fp = open(creds_path, 'rb')
      msg = MIMEText(fp.read(), _subtype=sub_type)
      fp.close()
      msg.add_header('Content-Disposition', 'attachment', filename=file)
      core_message.attach(msg)
    elif main_type == 'image':
      fp = open(creds_path, 'rb')
      msg = MIMEImage(fp.read(), _subtype=sub_type)
      fp.close()
      msg.add_header('Content-Disposition', 'inline', filename=file)
      msg.add_header('Content-ID', '<' + file + '>')
      main_message.attach(msg)
    elif main_type == 'audio':
      fp = open(creds_path, 'rb')
      msg = MIMEAudio(fp.read(), _subtype=sub_type)
      fp.close()
      msg.add_header('Content-Disposition', 'attachment', filename=file)
      core_message.attach(msg)
    elif main_type == 'application':   
      fp = open(creds_path, 'rb')
      msg = MIMEApplication(fp.read(), _subtype=sub_type)
      fp.close()
      msg.add_header('Content-Disposition', 'attachment', filename=file)
      core_message.attach(msg)
    else:
      fp = open(creds_path, 'rb')
      msg = MIMEBase(main_type, sub_type)
      msg.set_payload(fp.read())
      fp.close()
      msg.add_header('Content-Disposition', 'attachment', filename=file)
      core_message.attach(msg)
  if html_text != '':
    msgText = MIMEText(html_text, 'html')
    message.attach(msgText)
  return {'raw': base64.urlsafe_b64encode(core_message.as_string().encode()).decode()}

### SEND MAIL
def send_message(user_id, message):
  creds = None
  # If modifying these scopes, delete the file token.json.
  SCOPES = ['https://www.googleapis.com/auth/gmail.compose']
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.

  # Create the BlobServiceClient object which will be used to create a container client
  blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net')

  # Create a unique name for the container
  container_name = 'google-file'

  # Create a blob client using the local file name as the name for the blob
  blob_client = blob_service_client.get_blob_client(container=container_name, blob='gmail-tableau-token.json')
  creds_path = os.path.join(tempfile.gettempdir(), 'gmail-tableau-token.json')

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
  
  service = build('gmail', 'v1', credentials=creds)
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message
  except errors.HttpError:
    print('An error occurred')

### MAIN LOOP (READ LIST FROM GG SHEET AND PROCESS) ###
def run():
# If modifying these scopes, delete the file token.pickle.
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

  # The ID and range of a sample spreadsheet.
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

  RANGE_NAME = 'Mail!A:O'

  # Call the Sheets API
  today = datetime.now() + timedelta(hours = 7) + timedelta(minutes=-15)
  base = datetime(today.year , today.month , today.day , today.hour , today.minute , 0 , 0)
  sheet = service.spreadsheets()
  result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
  values = result.get('values', [])
  #Convert to Pandas Dataframe
  df = pd.DataFrame(values)
  df.columns = df.iloc[0]
  df.drop(df.index[0], inplace=True)
  df.drop(df[df.Enable != 'x'].index, inplace=True)
  groups = df.groupby('MailGroup')
  for name, group in groups:
    to = ''
    cc = ''
    bcc = ''
    Subject = ''
    message = ''
    m_from = ''
    iwidth = '500'
    file_list = list()
    valid = False
    for index, row in group.iterrows():
      if croniter.match(row['CRON'], base):
        valid = True
        to = row['to']
        cc = row['cc']
        bcc = row['bcc']
        iwidth = row['ImageWidth']
        Subject = row['Subject']
        if row['from'] == '':
          m_from = '"SKC, Dashboard"<skc_g.dashboard@kubota.com>'
        else:
          m_from = row['from']
        if row['ID'] != '':
          if row['type'] == 'file':
            file = gfileGET(row['ID'])
          if row['type'] == 'dashboard':
            if row['imageName'] == '':
              file = tableau_get_img(row['ID'],row['filterName'],row['filterValue'],'temp-'+str(index))
            else:
              file = tableau_get_img(row['ID'],row['filterName'],row['filterValue'],row['imageName'])
          if row['type'] == 'excel':
            if row['imageName'] == '':
              file = tableau_get_xls(row['ID'],row['filterName'],row['filterValue'],'temp-'+str(index))
            else:
              file = tableau_get_xls(row['ID'],row['filterName'],row['filterValue'],row['imageName'])
          if row['type'] == 'folder':
            file_list.extend(gfileGETfolder(row['ID']))
          else:
            file_list.append(file)
        txt_list = row['Content'].split('(nl)')
        message = ''
        for text in txt_list:
          message = message + text + '\n'
    if valid:
      msg = create_message_with_attachment(m_from,to,cc,bcc,Subject,message,file_list,iwidth)
      send_message('me',msg)