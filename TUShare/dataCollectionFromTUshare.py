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


def read_data_from_db(table_name):  # 从数据库中读取表，返回数据框类型
    sql = "SELECT * FROM %s LIMIT 20" % table_name
    df = pd.read_sql_query(sql, engine_ts)
    return df


def write_data_stock_list():  # 更新股票列表
    pro = ts.pro_api()
    df = pro.query(
        'stock_basic', fields='ts_code,symbol,name,area,industry,'
                              'market,list_date,exchange,curr_type,'
                              'delist_date,list_status,is_hs'
    )
    print("获取到的总记录数: ", len(df))
    if len(df) > 0:
        engine_ts.execute('delete from stock_basic where 1=1')
        res = df.to_sql('stock_basic', engine_ts, index=False, if_exists='append', chunksize=5000)
        print(res)


def write_data_index_list():  # 更新指数列表
    pro=ts.pro_api()
    df= pro.query(
        'index_basic',fields='ts_code,name,market,publisher,index_type,category,base_date,'
                             'base_point,exp_date'
    )
    print("获取到的总记录数: ", len(df))
    if len(df) > 0:
        engine_ts.execute('delete from index_basic where 1=1')
        res = df.to_sql('index_basic', engine_ts, index=False, if_exists='append', chunksize=5000)
        print(res)


def write_data_stock_company():  # 更新上市公司列表
    pro = ts.pro_api()
    df = pro.query(
        'stock_company', fields='ts_code,exchange,chairman,manager,secretary,'
                                'reg_capital,setup_date,province,city,'
                                'website,email,office,employees'
    )
    print("获取到的总记录数: ", len(df))
    if len(df) > 0:
        engine_ts.execute('delete from stock_company where 1=1')
        res = df.to_sql('stock_company', engine_ts, index=False, if_exists='append', chunksize=5000)
        print(res)


def write_data_stock_historic(stocklist, startdate, enddate):  # 更新股票的历史记录
    for stock in stocklist:
        try:
            df = ts.pro_bar(ts_code=stock, start_date=startdate, end_date=enddate,
                            asset='E', adj='qfq', freq='D')
            print(stock, " 获取到的总记录数: ", len(df))
            if len(df) > 0:
                engine_ts.execute("delete from stock_historic where ts_code=' %s' " % stock)
                df.to_sql('stock_historic', engine_ts, index=False, if_exists='append', chunksize=5000)
                print(stock, "操作成功！")
        except:
            print("股票%s操作不成功！" % stock)


def write_data_index_historic(indexlist, startdate, enddate):  # 更新指数历史记录
    pro = ts.pro_api()
    for index in indexlist:
        try:
            df = pro.index_daily(ts_code=index, start_date=startdate, end_date=enddate)

            print(index, " 获取到的总记录数: ", len(df))
            if len(df) > 0:
                # engine_ts.execute("delete from index_historic where ts_code=' %s' " % index)
                df.to_sql('index_historic', engine_ts, index=False, if_exists='append', chunksize=5000)
                print(index, "操作成功！")
        except:
            print("指数%s操作不成功！" % index)


def write_data_stock_daily(date):  # 更新指定日行情
    pro = ts.pro_api()
    df = pro.daily(trade_date=date)
    print(df)
    if len(df) > 0:
        print("获取到的总记录数: ", len(df))
        engine_ts.execute("delete from stock_daily where 1=1")
        df.to_sql('stock_daily', engine_ts, index=False, if_exists='append', chunksize=5000)
    else:
        print("未获取到任何记录")


def get_data(table_name):  # 读取tushare里面的表
    pro = ts.pro_api()
    df = pro.query(
        table_name
    )
    return df


def write_data_gdp():
    pro=ts.pro_api()
    df=pro.cn_gdp(start_q='2015Q1',end_q='2020Q3',fields='quarter,gdp,pi,si,ti')
    df.to_sql('gdp',engine_ts, index=False, if_exists='append', chunksize=5000)


def write_data_cpi():
    pro=ts.pro_api()
    df=pro.cn_cpi(start_m='201501',end_m='202009',fields='month,nt_val,town_val,cnt_val')
    df.to_sql('cpi',engine_ts, index=False, if_exists='append', chunksize=5000)


def write_data_ppi():
    pro=ts.pro_api()
    df=pro.cn_ppi(start_m='201501',end_m='202009',fields='month,ppi_mom,ppi_mp_mom,ppi_mp_qm_mom,ppi_mp_rm_mom,'
                                                         'ppi_mp_p_mom,ppi_cg_mom,ppi_cg_f_mom,ppi_cg_c_mom,'
                                                         'ppi_cg_adu_mom,ppi_cg_dcg_mom')
    df.to_sql('ppi',engine_ts, index=False, if_exists='append', chunksize=5000)


def write_data_money():
    pro=ts.pro_api()
    df=pro.cn_m(start_m='201501',end_m='202009',fields='month,m0,m1,m2')
    df.to_sql('money',engine_ts, index=False, if_exists='append', chunksize=5000)


def write_data_interest():
    pro=ts.pro_api()
    df=pro.shibor(start_date='20150101',end_date='20200930',fields='date,1y')
    df.to_sql('interest', engine_ts, index=False, if_exists='append', chunksize=5000)


def write_data_margin():
    pro=ts.pro_api()
    df=pro.margin(start_date='20150101',end_date='20200930')
    df.to_sql('margin', engine_ts, index=False, if_exists='append', chunksize=5000)


def write_data_stock_margin(stocklist, startdate, enddate):
    pro=ts.pro_api()
    i=len(stocklist)
    for stock in stocklist:
        print("开始操作股票",stock,"剩余:",i)
        df=pro.margin_detail(ts_code=stock,start_date=startdate,end_date=enddate)
        df.to_sql('stock_margin', engine_ts, index=False, if_exists='append', chunksize=5000)
        time.sleep(1)
        i=i-1


if __name__ == '__main__':
    print("开始操作...")
    # write_data_money()
    # write_data_gdp()
    # write_data_cpi()
    # write_data_ppi()
    # write_data_interest()
    # write_data_margin()
    # --------------循环抓取融资融券--------------
    # sql = "SELECT ts_code FROM stock_basic"
    # df = pd.read_sql_query(sql, engine_ts)
    # stocklist = list(df['ts_code'])
    # write_data_stock_margin(stocklist,'20050101','20201027')
    # ----------------test-------------------
    # pro=ts.pro_api()
    # df = pro.index_daily(ts_code='399300.SZ', start_date='20180101', end_date='20181010')
    # print(len(df))
    # ---------------------------------------
    # write_data_stock_list()  # 更新股票列表
    # write_data_index_list()  # 更新指数列表
    # write_data_stock_company()  # 更新上市公司列表

    # -----循环更新股票历史数据--------------------
    # stocklist = [
    #       '000002.SZ',
    #       '000005.SH',
    #       '000010.SZ',
    #       'test',
    # ]
    # sql = "SELECT ts_code FROM stock_basic"
    # df = pd.read_sql_query(sql, engine_ts)
    # stocklist = list(df['ts_code'])
    # write_data_stock_historic(stocklist, '20050101', '20201026')

    # write_data_stock_daily('20200916')  # 更新当日行情
    # get_data('daily')
    # --------------循环更新指数历史数据-------------------------
    # df=pd.read_sql_query("select ts_code from index_basic",engine_ts)
    # indexlist = list(df['ts_code'])
    # write_data_index_historic(indexlist,'20050101','20201027')



