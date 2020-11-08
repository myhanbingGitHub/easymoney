import time
from scrapy import signals
from selenium import webdriver
from scrapy.http.response.html import HtmlResponse


class ChromeMiddleware:

    def __init__(self):
        self.driver = webdriver.Chrome(executable_path=r"C:\MyFiles\chromedriver.exe")

    def process_request(self,request,spider):
        self.driver.get(request.url)
        print("浏览器开始访问页面...")
        time.sleep(10)

        # 先将整体页面拉到底，然后将小窗口里面拉到底
        js = "var q=document.documentElement.scrollTop=100000"
        self.driver.execute_script(js)
        time.sleep(1)
        # 尝试在小窗口内拉到底
        try:
            js = "var q=document.getElementById('F10MainTargetDiv').scrollTop=100000"
            self.driver.execute_script(js)
            time.sleep(1)
            # ---------点击年度报告页面抓取最近9年年度数据，如果注释掉下面3行，则抓取最近3年按季度
            js = "var q=document.querySelector('#zyzbTab > li:nth-child(2)').click()"
            self.driver.execute_script(js)
            time.sleep(1)
        except:
            print("没有F10MainTargetDiv")
        pagesource=self.driver.page_source
        r=HtmlResponse(url=self.driver.current_url,body=pagesource,request=request,encoding='utf-8')
        print("浏览器开始返回页面...")
        return r

