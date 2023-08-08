from numpy import NaN
import requests, os, json, tempfile
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient, __version__
from datetime import datetime, timezone, timedelta
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

def QUERY(theQuery,token):
    theToken = token

    url = "https://workflow.siamkubota.co.th/api/v3/reports/execute_query"
    headers = {"authtoken":theToken}
    data = {'input_data': theQuery}
    response = requests.post(url,headers=headers,data=data)
    response = json.loads(response.text)
    output = pd.DataFrame(response['execute_query']['data'])
    return output

def run():
  basedir = os.path.abspath(os.path.dirname(__file__))
  load_dotenv(os.path.join(basedir, '.env'))

#   theToken = os.getenv('token', None)
  theToken = '457CD416-5743-45D2-B47A-7BF022771FA0'
#   blobcon = os.getenv('blobcon', None)

  #Create scope
  scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

  # The ID and range of a sample spreadsheet.
  SPREADSHEET_ID = '1AZlUfS8YH0-raQyug8Z3ah0uNDr7f58z6gkPsPKrqWU'

  # Create the BlobServiceClient object which will be used to create a container client
  blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=dwhwebstorage;AccountKey=A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ==;EndpointSuffix=core.windows.net')

  # Download Credential File
#   blob_service_client = BlobServiceClient.from_connection_string(blobcon)
  container_name = 'google-file'

  # Create a blob client using the local file name as the name for the blob
  blob_client = blob_service_client.get_blob_client(container=container_name, blob='sheet-token.pickle')
  creds_path = os.path.join(tempfile.gettempdir(), 'sheet-token.pickle')
  
  with open(creds_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

  with open(creds_path, "rb") as token:
    creds = pickle.load(token)

  service = build('sheets', 'v4', credentials=creds)

  RANGE_NAME = 'Sheet1!A:O'

  # Call the Sheets API
  service = build('sheets', 'v4', credentials=creds)

  service = service.spreadsheets()
   
  sheet = service.values().get(spreadsheetId=SPREADSHEET_ID,range=RANGE_NAME).execute()

#   client = gspread.oauth(
#       credentials_filename=cred_file,
#       authorized_user_filename=auth_file,
#   )
#   sheet = client.open('HelpDeskDB').sheet1
  #Clear Unfinished Data
  sheet.clear()

  query_main = '''{
      "query": "SELECT * FROM (SELECT * FROM WorkOrder WHERE WorkOrder.ISPARENT='1') wo LEFT JOIN WorkOrderStates wos ON wo.WORKORDERID=wos.WORKORDERID "
      }'''
  query_td = '''{
      "query": "SELECT * FROM SDUser td LEFT JOIN AaaUser ti ON td.USERID=ti.USER_ID   "
      }'''
  query_qd = '''{
      "query": "SELECT * FROM WorkOrder_Queue woq LEFT JOIN QueueDefinition qd ON woq.QUEUEID=qd.QUEUEID   "
      }'''
  query_aad = '''{
      "query": "SELECT appsc.WORKORDERID, max(aaad.action_date) AS 'action_date' FROM  ApprovalStageMapping appsc LEFT JOIN ApprovalStage apps ON apps.APPROVAL_STAGEID=appsc.APPROVAL_STAGEID LEFT JOIN ApprovalDetails aaad ON aaad.APPROVAL_STAGEID=appsc.APPROVAL_STAGEID LEFT JOIN aaauser appsau ON appsau.user_id=aaad.approverid GROUP BY appsc.WORKORDERID  "
      }'''
  query_cd = '''{
      "query": "SELECT cd.CATEGORYNAME, cd.CATEGORYID FROM CategoryDefinition cd "
      }'''
  query_std = '''{
      "query": "SELECT std.STATUSID, std.STATUSNAME FROM StatusDefinition std"
      }'''
  query_scd = '''{
      "query": "SELECT scd.SUBCATEGORYID, scd.NAME AS 'SUB_NAME' FROM SubCategoryDefinition scd "
      }'''
  query_serdef = '''{
      "query": "SELECT serdef.SERVICEID, serdef.NAME AS 'SER_NAME' FROM ServiceDefinition serdef"
      }'''
  df_main = QUERY(query_main,theToken)
  df_td   = QUERY(query_td,theToken)
  df_qd   = QUERY(query_qd,theToken)
  df_aad  = QUERY(query_aad,theToken)
  df_cd  = QUERY(query_cd,theToken)
  df_std  = QUERY(query_std,theToken)
  df_scd  = QUERY(query_scd,theToken)
  df_serdef  = QUERY(query_serdef,theToken)
  print('Merging')
  df_main.rename(columns = {'CREATEDTIME':'TIMECREATED'}, inplace = True)
  df_main.rename(columns = {'COMPLETEDTIME':'TIMECOMPLETED'}, inplace = True)
  df_output = pd.merge(df_main, df_td, how='left', left_on = 'OWNERID', right_on = 'USERID')
  df_output = pd.merge(df_output, df_qd, how='left', left_on = 'WORKORDERID', right_on = 'WORKORDERID')
  df_output = pd.merge(df_output, df_aad, how='left', left_on = 'WORKORDERID', right_on = 'WORKORDERID')
  df_output = pd.merge(df_output, df_cd, how='left', left_on = 'CATEGORYID', right_on = 'CATEGORYID')
  df_output = pd.merge(df_output, df_std, how='left', left_on = 'STATUSID', right_on = 'STATUSID')
  df_output = pd.merge(df_output, df_scd, how='left', left_on = 'SUBCATEGORYID', right_on = 'SUBCATEGORYID')
  df_output = pd.merge(df_output, df_serdef, how='left', left_on = 'SERVICEID', right_on = 'SERVICEID')

  df_fin = df_output.loc[:,['WORKORDERID', 'RESPONDEDTIME' , 'ASSIGNEDTIME', 'FIRST_NAME', 'QUEUENAME', 'TITLE', 'IS_CATALOG_TEMPLATE', 'SUB_NAME', 'CATEGORYNAME', 'SER_NAME', 'STATUSNAME', 'action_date', 'TIMECREATED',  'RESOLVEDTIME',  'TIMECOMPLETED' ]]
  df_fin.rename(columns = {'WORKORDERID':'Request ID', 'RESPONDEDTIME':'Responded Time' , 'ASSIGNEDTIME':'Assigned Time', 'FIRST_NAME':'Technician', 'QUEUENAME':'Group', 'TITLE':'Subject', 'IS_CATALOG_TEMPLATE':'Service Request', 'SUB_NAME':'Subcategory', 'CATEGORYNAME':'Category', 'SER_NAME':'Service Category', 'STATUSNAME':'Request Status', 'action_date':'Action Date', 'TIMECREATED':'Created Time',  'RESOLVEDTIME':'Resolved Time',  'TIMECOMPLETED':'Completed Time'}, inplace = True)

  if not df_output.empty:
      df_fin['Created Time'] = df_fin.apply(lambda row : None if pd.isnull(row['Created Time']) else datetime.fromtimestamp(row['Created Time']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
      df_fin['Assigned Time'] = df_fin.apply(lambda row : None if pd.isnull(row['Assigned Time']) else datetime.fromtimestamp(row['Assigned Time']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
      df_fin['Completed Time'] = df_fin.apply(lambda row : None if (pd.isnull(row['Completed Time']) | row['Completed Time'] == 0) else datetime.fromtimestamp(row['Completed Time']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
      df_fin['Resolved Time'] = df_fin.apply(lambda row : None if (pd.isnull(row['Resolved Time']) | row['Resolved Time'] == 0) else datetime.fromtimestamp(row['Resolved Time']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
      df_fin['Responded Time'] = df_fin.apply(lambda row : None if (pd.isnull(row['Responded Time']) | row['Responded Time'] == 0) else datetime.fromtimestamp(row['Responded Time']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
      df_fin['Action Date'] = df_fin.apply(lambda row : None if (pd.isnull(row['Action Date'])) else datetime.fromtimestamp(row['Action Date']/1000,timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S'), axis = 1)
      df_fin['Reject Time'] = df_fin.apply(lambda row : None if (row['Request Status'] != 'Reject') else row['Action Date'], axis=1)
      df_fin.drop('Action Date', axis=1, inplace=True)
      df_fin = df_fin.fillna('')
#   service.values().update([df_fin.columns.values.tolist()] + df_fin.values.tolist())
  logs = [df_fin.columns.values.tolist()] + df_fin.values.tolist()
  service.values().update(spreadsheetId=SPREADSHEET_ID,
                        range=RANGE_NAME,valueInputOption='USER_ENTERED',
                        body=dict(majorDimension='ROWS',
                        values=logs)).execute()