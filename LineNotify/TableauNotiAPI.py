# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 13:52:56 2021

@author: methee.s
"""
import requests
import logging
import time

def GetAuth():
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    urlHis = server + "auth/signin"
    headers = {"Content-Type": "application/json",
               "Accept":"application/json"}
    payload = { "credentials": {
                        		"personalAccessTokenName": "LineNotifyExcel",
                        		"personalAccessTokenSecret": "v4eRTldsQ4qJvNFeYQE6AA==:fSKPlT1P40DC4IxAKRX6IHsrUGjio1ld",
                        		"site": {
                        			"contentUrl": "skctableau"
                        		}
                }
        }
    res = requests.post(urlHis, headers=headers, json = payload)
    response =  res.json()
    token = response['credentials']['token']
    site_id = response['credentials']['site']['id']
    return token,site_id

def GetViewId(dashboard,headers,site_id):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    url = server + '/sites/'+site_id+'/views?filter=viewUrlName:eq:' + dashboard
    res = requests.get(url, headers=headers, json = {})
    response =  res.json()
    if len(response['views']) == 0 :
        return '','','',''
    elif len(response['views']['view']) > 1 :
        return response['views']['view'][0]['id']
    else :
        return response['views']['view'][0]['id']

def GetImage(dashboard,Id,filterName,filterValue,LineToken,message,auth,site_id):
    server = 'https://prod-apnortheast-a.online.tableau.com/api/3.19/'
    headers = {"Content-Type": "application/json",
               "Accept":"application/json",
               "X-Tableau-Auth": auth}
    if dashboard == 'MESSAGE':
        LineUrl = 'https://notify-api.line.me/api/notify'
        #LineToken = 'QDd6ExB9L9onVWb2sze4DfStpiKHB6DXTVCpV2teXEk'
        LineHeaders = {'Authorization':'Bearer '+ LineToken}
        payload = {'message':message}
        resp = requests.post(LineUrl, headers=LineHeaders , data = payload)
        time.sleep(3)
    else:
        if Id != '':
            view_id = Id
        else:
            view_id = GetViewId(dashboard,headers,site_id)
        if filterName == '':
            url = server +  '/sites/'+site_id+'/views/'+view_id+'/image' + '?maxAge=1'+'&resolution=high'
        else:
            url = server +  '/sites/'+site_id+'/views/'+view_id+'/image' + '?vf_'+filterName+'='+filterValue+'&maxAge=1'+'&resolution=high&sort=ส่วน:asc'
        #url = server +  '/sites/'+site_id+'/views/'+view_id+'/image' + '?vf_สายงาน=สายงานวางแผนและควบคุม/CFO'
        res = requests.get(url, headers=headers, json = {})
        print(view_id)
        #Send to Line
        LineUrl = 'https://notify-api.line.me/api/notify'
        #LineToken = 'QDd6ExB9L9onVWb2sze4DfStpiKHB6DXTVCpV2teXEk'
        LineHeaders = {'Authorization':'Bearer '+ LineToken}
        payload = {'message':message}
        file = {'imageFile':res.content}
        resp = requests.post(LineUrl, headers=LineHeaders , data = payload , files = file)
        logging.info(resp)
        time.sleep(3)

