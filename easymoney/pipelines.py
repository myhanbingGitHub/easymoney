# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
from pandas.core.frame import DataFrame
import pymysql
from datetime import datetime
import json


class EasymoneyPipeline:
    def __init__(self):
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
        self.cnn = pymysql.connect(**cnn_param)
        self.cur = self.cnn.cursor()

    def process_item(self, item, spider):
        print("为%s页存储数据:" % item['ItemSource'])

        if item['ItemSource'] == "Fin" and len(item['content']) > 0:  # 说明抛过来的item是财务页面
            d = item['content']
            # 构建list
            # 将list转成df并处理
            df = DataFrame(d)
            header = item['content'][0]
            header[0] = "指标名称"
            header[-1] = '标记'
            df.columns = header
            df.drop(df[df['标记'] == 'th'].index, inplace=True)
            df.drop(['标记'], axis=1, inplace=True)
            df.set_index(['指标名称'], inplace=True)
            print(df)
            sr = df.stack()
            if len(df) > 0:
                # self.cur.execute("delete from stock_analysis where code='%s'" % item['code'])
                # self.cnn.commit()
                # print("删除stock_analysis现有记录%s成功", item['code'])
                sql = "select max(period),min(period) from stock_analysis where code='%s'" % item['code']
                print(sql)
                self.cur.execute(sql)
                maxmindate = self.cur.fetchone()
                if maxmindate[0]:
                    maxdate = maxmindate[0]
                    mindate = maxmindate[1]
                    print("该股票现有数据库最大日期为:%s,最小日期为:%s" %(maxdate,mindate))
                else:
                    maxdate = datetime.strptime("2000-01-01", "%Y-%m-%d").date()
                sql = "insert into stock.stock_analysis (code, kpi, period, value, url) " \
                      "values (%s,%s,%s,%s,%s)"
                for i in sr.index:
                    try:
                        if datetime.strptime('20' + i[1], "%Y-%m-%d").date() > maxdate or \
                                datetime.strptime('20' + i[1], "%Y-%m-%d").date() < mindate:
                            self.cur.execute(sql,
                                             (item['code'], i[0], datetime.strptime('20' + i[1], "%Y-%m-%d"), sr[i],
                                              item['url']
                                              ))
                            self.cnn.commit()
                            print(item['code'], "-", i[0], "-", "20" + i[1], "-", sr[i], "记录已入库")
                        else:
                            print("记录已经存在，不用入库。")
                    except:
                        self.cnn.rollback()
                        print("记录入库失败！")
        elif item['ItemSource'] == "KPI":  # 指标页面过来数据
            try:
                self.cur.execute("delete from stock_kpi where code='%s'" % item['code'])
                self.cnn.commit()
                print("删除stock_kpi现有记录%s成功", item['code'])
            except:
                print("删除旧记录不成功")

            sql = "insert into stock_kpi (code, totalValue, netValue, NPValue, PE, BE, GP, NP, ROE," \
                  "totalValue_rank, netValue_rank, netProfit_rank, PE_rank, BE_rank, GP_rank, NP_rank, ROE_rank) " \
                  "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:
                self.cur.execute(sql, (
                    item['code'],
                    item['totalValue'],
                    item['netValue'],
                    item['NPValue'],
                    item['PE'],
                    item['BE'],
                    item['GP'],
                    item['NP'],
                    item['ROE'],
                    item['totalValue_rank'],
                    item['netValue_rank'],
                    item['netProfit_rank'],
                    item['PE_rank'],
                    item['BE_rank'],
                    item['GP_rank'],
                    item['NP_rank'],
                    item['ROE_rank']
                ))
                self.cnn.commit()
                print(item['code'], "入库stock_kpi成功")
            except:
                print(item['code'], "入库stock_kpi失败!")
        elif item['ItemSource'] == "Money":  # 资金页面过来数据
            print(item['code'])
            sql= "select max(date) from stock_money where code='%s'" % item['code']
            self.cur.execute(sql)
            maxdate = self.cur.fetchone()
            print('maxdate:', maxdate)
            if maxdate[0]:
                maxdate = maxdate[0]
            else:
                maxdate = datetime.strptime("2000-01-01","%Y-%m-%d").date()
            for i in item['MoneyList']:
                if datetime.strptime(i[0], "%Y-%m-%d").date() > maxdate:
                    sql = "insert into stock_money (code, date, main, superBig, big, medium, small) " \
                          "values ('%s','%s','%s','%s','%s','%s','%s' )" % (item['code'],
                                                                            datetime.strptime(i[0], "%Y-%m-%d").date(),
                                                                            i[3], i[5], i[7], i[9], i[11])
                    # print(sql)
                    try:
                        self.cur.execute(sql)
                        self.cnn.commit()
                        print(item['code'], "日期", i[0], "入库stock_money成功")
                    except:
                        print(item['code'], "日期", i[0], "入库stock_money失败")
        return None
