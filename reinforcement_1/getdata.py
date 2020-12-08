import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import time
import datetime
import json

with open('../parameters.json', 'r') as f:

    p = json.load(f)
    user = p['user']
    port = p['port']
    psw = p['password']
    host = p['host']
    token = p['TU_share_pro_taken']
    cnnstr = "mysql://" + user + ":" + psw + "@" + host + ":" + str(port) + "/stock?charset=utf8&use_unicode=1"

engine_ts = create_engine(cnnstr)
ts.set_token(token)
# print(token)

stock = '000001.SZ'
startdate = '20200101'
enddate='20201116'
df = ts.pro_bar(ts_code=stock, start_date=startdate, end_date=enddate,
                            asset='E', adj='qfq', freq='15min')

print(df)