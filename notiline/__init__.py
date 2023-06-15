import datetime
import logging
import requests
import azure.functions as func
from . import linenoti

def func_LineNotify(Message,LineToken):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    return response

today = datetime.datetime.today()
todatStr = today.strftime('%Y-%m-%d, %H:%M:%S')

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    func_LineNotify('testtestsetset : '+ todatStr,'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e')