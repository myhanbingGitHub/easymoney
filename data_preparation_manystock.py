import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import time
import datetime
import json
import numpy as np

# 导入token
with open('./parameters.json','r') as f:
    p=json.load(f)
    user = p['user']
    port = p['port']
    psw = p['password']
    host = p['host']
    token=p['TU_share_pro_taken']
    cnnstr = "mysql://" + user + ":" + psw + "@" + host + ":" + str(port) + "/stock?charset=utf8&use_unicode=1"
engine_ts = create_engine(cnnstr)
ts.set_token(token)


# ----------参数------------------
path_save = './DataAnalysis/many_stock/'
stocklist=list(pd.read_csv('./DataAnalysis/stock_list.csv').iloc[:,0])
start='20060101'
end='20201125'
backwards = 66
foresee = 5
max_pe=100*1000
max_pb=100*1000
max_mv=100*1000*1000*1000
split=0.8


# 读取宏观数据
Marco=pd.read_csv('./DataAnalysis/Macro2016_20201031.csv')

# 处理宏观数据
df3=Marco
## 处理将所有值的部分除以100
df3=df3.set_index('YYYYMMDD')
df3=df3.apply(lambda x: x/100)
df3.reset_index(inplace=True)


# 处理单片数据
def data_calculation(df):   # df 为数据框格式
    dfa=df.copy()
    dfa.reset_index(drop=True,inplace=True)   # 重新索引
    dfa.insert(dfa.shape[1],'factor',np.nan)  #  增加复权列
    dfa.loc[0,'factor']=1   #  设置起始复权值为1
    for i in range(1,len(dfa)):   #  循环计算后续复权值
        dfa.loc[i,'factor']=dfa.loc[i-1,'factor']*(1+dfa.loc[i,'price_change'])
    # 重新计算开高低收的值
    dfa['open2']=dfa['open']/dfa['close']*dfa['factor']
    dfa['high2']=dfa['high']/dfa['close']*dfa['factor']
    dfa['low2']=dfa['low']/dfa['close']*dfa['factor']
    ts_code=dfa.loc[0,'ts_code']
    trade_date=dfa.loc[backwards-1,'trade_date']
    dfa.drop(columns=['ts_code','trade_date','open','high','low','close','pre_close','price_change'],inplace=True)  # 删除多余列
    #  计算标签值
    label=dfa.loc[backwards:backwards+foresee-1,'factor'].max()/dfa.loc[backwards-1,'factor']-1
    dfa.insert(dfa.shape[1],'label_value',label)
    # 删除尾部5行，确保X值在100
    dfa.drop(dfa.tail(foresee).index,inplace=True)
    #  返回数据框以及标签值
    return dfa, ts_code,trade_date


def individual_stock(ts_code):
    # 获取股票价格历史
    daily = ts.pro_bar(ts_code=ts_code, start_date=start, end_date=end,
                            asset='E', adj='qfq', freq='D')
    daily.sort_values(by='trade_date',inplace=True)
    # 获取股票日KPI
    pro = ts.pro_api()
    daily_basic=pro.daily_basic(ts_code=ts_code, start_date=start, end_date=end,
                            fields='ts_code,trade_date,turnover_rate,pe_ttm,pb,total_mv')
    daily_basic.sort_values(by='trade_date',inplace=True)
    # 处理价格历史
    df1=daily
    df1.reset_index(drop=True,inplace=True)
    df1['price_change']=(df1['close']-df1['pre_close'])/df1['pre_close']
    df1.drop(columns=['vol','amount','change','pct_chg'],inplace=True)
    # 处理日KPI
    df2=daily_basic
    ## 处理指标，都除以最大数字
    df2['pe_ttm']=df2.pe_ttm.apply(lambda x: x/max_pe)
    df2['pb']=df2.pb.apply(lambda x: x/max_pb)
    df2['total_mv']=df2.total_mv.apply(lambda x: x/max_mv)
    # 合并数据
    df=pd.merge(df1,df2,how='left',on=['ts_code','trade_date'],left_index=False,right_index=False,copy=True)
    df['trade_date']=df['trade_date'].astype('int64')
    # 合并宏观数据
    df=pd.merge(df,df3,how='left',left_on='trade_date',right_on='YYYYMMDD',left_index=False,right_index=False,copy=True)
    # 处理合并后的数据
    df.fillna(method='bfill',axis=0,inplace=True) # 纵向填充，用后面的值填充前面，用以补充最早缺失的数据/数据排序为从早到晚
    df.fillna(method='ffill',axis=0,inplace=True) # 纵向填充，用前面的值填充后面，用以补充最近缺失的数据/数据排序为从早到晚
    df.drop(columns=['YYYYMMDD'],inplace=True)
    # 循环存入单片数据
    for i in range(len(df)-backwards-foresee):
        ddd,ts_code,trade_date = data_calculation(df[i:i+backwards+foresee])
        ddd.to_csv(path_or_buf=path_save+ts_code+'_'+str(trade_date)+'.csv',sep=',',index=False)
        print(f'stock:{ts_code},处理完毕：{round(i/(len(df)-backwards-foresee)*100,2)} %')
    print(f'stock:{ts_code}处理完毕！')

i = 0
for ts_code in stocklist:
    individual_stock(ts_code)
    i = i + 1
    print(f'完成率：{round(i/len(stocklist)*100,2)} %')
