import pandas as pd
from sqlalchemy import create_engine


engine= create_engine('mysql://stock:Stock_789@192.168.88.106:3306/stock?charset=utf8&use_unicode=1')

sql = "select T1.kpi,T1.period,T1.code,T1.value, T2.category from stock_analysis T1 left join kpi_category T2" \
      " on T1.kpi=T2.kpiName where period=(select max(period) from stock_analysis);"
df = pd.read_sql_query(sql, engine)
print(df)