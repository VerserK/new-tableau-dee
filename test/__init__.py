import datetime
from datetime import timedelta
import logging
import requests
import azure.functions as func

def func_LineNotify(Message,LineToken):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    return response 

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    today = datetime.datetime.today() + timedelta(hours=7)
    todatStr = today.strftime('%Y-%m-%d, %H:%M:%S')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    func_LineNotify('all 5 min : '+ todatStr,'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e')