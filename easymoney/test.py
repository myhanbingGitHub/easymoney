import pymysql
import json
from datetime import datetime


with open('../parameters.json', 'r') as f:
    p = json.load(f)

cnn_param = {
    'host': p['host'],
    'port': p['port'],
    'user': p['user'],
    'password': p['password'],
    'database': 'stock',
    'charset': 'utf8'
}
cnn = pymysql.connect(**cnn_param)
cur = cnn.cursor()

sql= "select max(date) from stock_money where code='600335'"
cur.execute(sql)
maxdate = cur.fetchone()[0]
print(maxdate, type(maxdate))
d=datetime.strptime('2020-10-20','%Y-%m-%d').date()
print(d, type(d))
if d > maxdate:
    print("大于")
else:
    print("小于等于")

cur.close()
cnn.close()