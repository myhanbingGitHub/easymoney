import scrapy
from lxml import etree
from easymoney.items import EasymoneyItem
from easymoney.get_todolist import get_todo_list
from easymoney.get_todolist import get_todo_list2
from easymoney.get_todolist import get_todo_list3


class EasymoneySpdSpider(scrapy.Spider):
    # 构造财务页list
    todolist = get_todo_list()
    todourl = []
    for todo in todolist:
        todourl.append(
            'http://f10.eastmoney.com/f10_v2/FinanceAnalysis.aspx?code=' + todo
        )
    print("需要爬取财务页面：", len(todourl))
    # 构造指标页list
    todolist2 = get_todo_list2()
    todourl2 = []
    for todo in todolist2:
        todourl2.append(
            'http://quote.eastmoney.com/' + todo + '.html'
        )
    print("需要爬取主页面：", len(todourl2))
    # 构造资金流页面list
    todolist3 = get_todo_list3()
    todourl3 = []
    for todo in todolist3:
        todourl3.append(
            'http://data.eastmoney.com/zjlx/' + todo + '.html'
        )
    print("需要爬取资金流页面：", len(todourl3))
    todourl = todourl + todourl2 + todourl3
    c = len(todourl)  # 计算待爬取的页数
    print("需要爬取总页面数：", c)
    print(todourl)
    name = 'easymoney_spd'
    allowed_domains = ['eastmoney.com']
    # start_urls = todourl

    # start_urls = ['http://data.eastmoney.com/zjlx/000001.html']
    # 'http://quote.eastmoney.com/sz300750.html'
    start_urls = ['http://f10.eastmoney.com/f10_v2/FinanceAnalysis.aspx?code=sz000001']

    def parse(self, response):
        # print(response.url[7:10])
        # print(response.url[7:12])
        item = EasymoneyItem()
        if response.url[7:10] == "f10":
            print("开始爬取财务页面")
            html = etree.HTML(response.text)
            table = html.xpath("//div[@id='report_zyzb']/table/tbody/tr")
            l = []
            for row in table:
                th = row.xpath(".//th/span/text()")
                td = row.xpath(".//td/span/text()")
                if th:
                    th.append('th')
                    l.append(th)
                elif td:
                    td.append('td')
                    l.append(td)
            item['url'] = response.url
            item['code'] = response.url[-6:]
            item['content'] = l
            item['ItemSource'] = 'Fin'
            print(item)
        else:
            pass

        if response.url[7:12] == "quote":
            print("开始爬取指标页面")
            html = etree.HTML(response.text)
            k1 = html.xpath("//*[@id='cwzbDataBox']/tr[1]/td/text()")
            k2 = html.xpath("//*[@id='cwzbDataBox']/tr[3]/td/text()")
            print(k1)
            print(k2)
            item['code'] = response.url[-11:-5]
            item['totalValue'] = k1[1]
            item['netValue'] = k1[2]
            item['NPValue'] = k1[3]
            item['PE'] = k1[4]
            item['BE'] = k1[5]
            item['GP'] = k1[6]
            item['NP'] = k1[7]
            item['ROE'] = k1[8]
            item['totalValue_rank'] = k2[1]
            item['netValue_rank'] = k2[2]
            item['netProfit_rank'] = k2[3]
            item['PE_rank'] = k2[4]
            item['BE_rank'] = k2[5]
            item['GP_rank'] = k2[6]
            item['NP_rank'] = k2[7]
            item['ROE_rank'] = k2[8]
            item['ItemSource'] = 'KPI'
        else:
            pass

        if response.url[26:30] == "zjlx":
            print("开始爬取资金流页面")
            html = etree.HTML(response.text)
            tab = html.xpath("//*[@id='content_zjlxtable']/table/tbody/tr")
            # 将数据包装到listT里面，并通过item传递回去
            listT=[]
            for r in tab:
                listT.append(r.xpath(".//td/text()")+r.xpath(".//td/span/text()"))
            item['MoneyList'] = listT
            item['code'] = response.url[-11:-5]
            item['ItemSource'] = 'Money'
        return item
