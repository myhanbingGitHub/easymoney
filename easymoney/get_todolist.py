import pymysql
import json


def get_todo_list():
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
    # 抓取100天内没有被更新过的股票列表
    sql = "  select T1.ts_code, T2.dat from stock_basic T1 left join (select code, max(modified) as dat " \
          "from stock_analysis where 1=1 group by code) T2 on T1.symbol=T2.code " \
          "where t2.dat is null or datediff(now(),T2.dat)>=7;"

    cur.execute(sql)
    rs = cur.fetchall()
    todolist = []
    for r in rs:
        sc = str.lower(r[0].split(".")[-1])  # 将结果加工成sh0000001 这种形式
        code = r[0].split(".")[0]
        todolist.append(sc+code)
    cur.close()
    cnn.close()

    return todolist


def get_todo_list2():
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
    # 抓取30天内没有被更新过的股票列表
    sql = "  select T1.ts_code, T2.dat from stock_basic T1 left join (select code, max(updated) as dat " \
          "from stock_kpi where 1=1 group by code) T2 on T1.symbol=T2.code " \
          "where t2.dat is null or datediff(now(),T2.dat)>=30;"

    cur.execute(sql)
    rs = cur.fetchall()
    todolist = []
    for r in rs:
        sc = str.lower(r[0].split(".")[-1])  # 将结果加工成sh0000001 这种形式
        code = r[0].split(".")[0]
        todolist.append(sc+code)
    cur.close()
    cnn.close()

    return todolist


def get_todo_list3():
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
    # 抓取30天内没有被更新过的股票列表
    sql = "  select T1.ts_code, T2.dat from stock_basic T1 left join (select code, max(modification) as dat " \
          "from stock_money where 1=1 group by code) T2 on T1.symbol=T2.code " \
          "where t2.dat is null or datediff(now(),T2.dat)>=30;"

    cur.execute(sql)
    rs = cur.fetchall()
    todolist = []
    for r in rs:
        code = r[0].split(".")[0]
        todolist.append(code)
    cur.close()
    cnn.close()

    return todolist