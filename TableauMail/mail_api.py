# -*- coding: utf-8 -*-
"""
Created on Thu Aug 26 19:51:01 2021

@author: methee.s
"""
from __future__ import print_function

import requests
import pickle
import pandas as pd
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from datetime import datetime

import base64
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import mimetypes
import os
import tempfile
import io
from io import StringIO

from apiclient import errors

import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

def tableau_get_xls(view_id,fName,fValue,dbName):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "TableauMail",
        		"personalAccessTokenSecret": "qBIrvQa8SCSicSiSZ9zUtw==:VUg9T3q9UQwXb2JfgEJe3Ie8TgBftDlp",
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
    url = server +  '/sites/'+site_id+'/views/'+view_id+'/crosstab/excel?vf_{0}={1}'.format(fName,fValue)
    print(url)
    #res = requests.get(url, headers=headers, json = {})
    res = requests.get(url, headers=headers, allow_redirects=True)
    filename = dbName+'-'+fValue+'.xlsx'
    creds_path = os.path.join(tempfile.gettempdir(), filename)
    file = open(creds_path, "wb")
    file.write(res.content)
    file.close()
    return filename

def tableau_get_data(view_id,fName,fValue,dbName):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "TableauMail",
        		"personalAccessTokenSecret": "qBIrvQa8SCSicSiSZ9zUtw==:VUg9T3q9UQwXb2JfgEJe3Ie8TgBftDlp",
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
    url = server +  '/sites/'+site_id+'/views/'+view_id+'/data?vf_{0}={1}'.format(fName,fValue)
    print(url)
    #res = requests.get(url, headers=headers, json = {})
    res = requests.get(url, headers=headers, allow_redirects=True)
    filename = dbName+'-'+fValue+'.csv'
    creds_path = os.path.join(tempfile.gettempdir(), filename)
    file = open(creds_path, "wb")
    file.write(res.content)
    file.close()
    return filename

def tableau_get_img(view_id,fName,fValue,dbName):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
        		"personalAccessTokenName": "TableauMail",
        		"personalAccessTokenSecret": "qBIrvQa8SCSicSiSZ9zUtw==:VUg9T3q9UQwXb2JfgEJe3Ie8TgBftDlp",
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
    url = server +  '/sites/'+site_id+'/views/'+view_id+'/image?vf_{0}={1}'.format(fName,fValue)
    print(url)
    res = requests.get(url, headers=headers, json = {})
    filename = dbName+'.jpeg'
    creds_path = os.path.join(tempfile.gettempdir(), filename)
    file = open(creds_path, "wb")
    file.write(res.content)
    file.close()
    return filename

def create_message_with_attachment(
    sender, to, cc, bcc, subject, message_text, file_list):
  core_message = MIMEMultipart('mixed')
  core_message['to'] = to
  if cc != '':
      core_message['cc'] = cc
  if bcc != '':
      core_message['bcc'] = bcc
  core_message['from'] = sender
  core_message['subject'] = subject

  main_message = MIMEMultipart('related')
  core_message.attach(main_message)
  message = MIMEMultipart('alternative')
  main_message.attach(message)
  
  txt_list = message_text.split('\n')
  html_text = ''
  for text in txt_list:
    html_text = html_text + '<div><p>' + text + '</p></div>'

  for file in file_list:
    content_type, encoding = mimetypes.guess_type(file)
    creds_path = os.path.join(tempfile.gettempdir(), file)
    if content_type is None or encoding is not None:
      content_type = 'application/octet-stream'
    main_type, sub_type = content_type.split('/', 1)
    if main_type == 'text':
      fp = open(creds_path, 'rb')
      msg = MIMEText(main_type, sub_type, _charset = 'UTF-8')
      msg.set_payload(fp.read())
      fp.close()
    elif main_type == 'image':
      html_text = html_text + '<div><img src="cid:' + file + '" width=500><br></div>'
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
    elif main_type == 'application':   
      fp = open(creds_path, 'rb')
      msg = MIMEApplication(fp.read(), _subtype=sub_type)
      fp.close()
    else:
      fp = open(creds_path, 'rb')
      msg = MIMEBase(main_type, sub_type)
      msg.set_payload(fp.read())
      fp.close()
    
    if main_type != 'image':
      msg.add_header('Content-Disposition', 'attachment', filename=file)
      core_message.attach(msg)
    # with open(os.path.join(tempfile.gettempdir(), file), 'rb') as f:
    #             file_data = f.read()
    #             file_name = f.name
    #msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)
  if html_text != '':
    msgText = MIMEText(html_text, 'html')
    message.attach(msgText)
  return {'raw': base64.urlsafe_b64encode(core_message.as_string().encode()).decode()}

def send_message(service, user_id, message):
  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print('Message Id: %s' % message['id'])
    return message
  except errors.HttpError:
    print('An error occurred')
def mail_service():
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
  return service

def run():
  # If modifying these scopes, delete the file token.pickle.
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

  # The ID and range of a sample spreadsheet.
  SPREADSHEET_ID = '1Xrz0fm8-XbzGKMTJRMdKckWIVsJrUNjV6O-YIJnuPZg'

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

  RANGE_NAME = 'Sheet1!A:M'

  # Call the Sheets API
  sheet = service.spreadsheets()
  result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()
  values = result.get('values', [])
  today = datetime.now()
  theHour = today.hour + 7
  if theHour >= 24:
    theHour = theHour - 24
  #Convert to Pandas Dataframe
  df = pd.DataFrame(values[1:], columns=values[0])
  df.columns = ['Enable','MailGroup','Workbook','Dashboard','DashboardID','Format','FilterField','FilterValue','to','cc','bcc','Subject','Content']
  df = df[df['DashboardID'].notna()]
  df = df.drop(df[df.Enable != 'x'].index)
  groups = df.groupby('MailGroup')
  for name, group in groups:
    to = ''
    cc = ''
    bcc = ''
    Subject = ''
    message = ''
    file_list = list()
    for index, row in group.iterrows():
      to = row['to']
      cc = row['cc']
      bcc = row['bcc']
      Subject = row['Subject']
      if row['Format'] == 'img':
        file = tableau_get_img(row['DashboardID'],row['FilterField'],row['FilterValue'],row['Dashboard'])
      elif row['Format'] == 'data':
        file = tableau_get_data(row['DashboardID'],row['FilterField'],row['FilterValue'],row['Dashboard'])
      elif row['Format'] == 'excel':
        file = tableau_get_xls(row['DashboardID'],row['FilterField'],row['FilterValue'],row['Dashboard'])
      file_list.append(file)
      txt_list = row['Content'].split('(nl)')
      message = ''
      for text in txt_list:
        message = message + text + '\n'
    msg = create_message_with_attachment('Construction Business <thitima.y@kubota.com>',to,cc,bcc,Subject,message,file_list)
    service = mail_service()
    send_message(service,'me',msg)