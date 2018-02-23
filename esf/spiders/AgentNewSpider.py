# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem, DistrictItem,IndexItem
from scrapy.loader import ItemLoader

from urllib.parse import urlparse,urlencode
import socket
import datetime
import sqlite3
import re


class AgentDistrictSpider(scrapy.Spider):
    name = "AgentDistrictSpider"
    category = "agency"
    start_urls = get_project_settings().get("CATEGORIES")[category]

    def __init__(self,*args,**kwargs):
        super(AgentDistrictSpider,self).__init__()
        # 如何setting 中定义refresh则重新刷新部分页面
        if get_project_settings().get("REFRESH_URLS"):
            self.logger.critical("refresh partial urls")
            self.start_urls = self.fresh_urls()

    def fresh_urls(self):
        with sqlite3.connect("data/esf_urls_test.db") as cnx:
            cursor = cnx.cursor()
            cursor.execute("select DISTINCT source from district where subdistrict = 'nodef'")
            urls = cursor.fetchall()
            cursor.executemany("DELETE from district where source = ?",urls)
            return [r[0] for r in urls]

    def parse(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        district_urls = []
        ###  得到区域列表
        # 5a5j
        if not district_urls:
            self.logger.info("5a5j district ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//ancestor::ul[1]//a[not(.//text()="全部")]')
        # lianjia
        if not district_urls:
            self.logger.info("lianjia district ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::div[@class="option-list"]//a[not(text()="不限")]')
        # ganji
        if not district_urls:
            self.logger.info("ganji/fangdd_esf/qfang/netease district ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::ul//a[not(text()="不限")]')
        # 58 商铺
        if not district_urls:
            self.logger.info("58 district ...")
            district_urls = response.xpath('//*[contains(text(),"全上海")]//..//a[not(contains(text(),"全上海"))]')
        # 安居客
        if not district_urls:
            self.logger.info("anjuke district ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//ancestor::span[@class="elems-l"]//a[not(.//text()="全部")]')
        # fang
        if not district_urls:
            self.logger.info("fang district ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::div[@class="qxName"]//a[not(text()="不限")]')
        # centanet
        if not district_urls:
            self.logger.info("centanet district ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::p[@class="termcon fl"]//a[not(text()="不限")]')
        ###

        if not district_urls:
            self.logger.error("!!!! url: %s not found any districts, checkout again this  !!!!", response.url)
            l = ItemLoader(item=DistrictItem())
            l.default_output_processor = TakeFirst()
            l.add_value("district", "nodef")
            l.add_value("subdistrict", "nodef")
            l.add_value("url", response.url)
            l.add_value("category", self.category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

        for url in district_urls:
            district_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            district_name = "".join(url.xpath('.//text()').extract()).strip()

            yield Request(url=district_url, callback=self.parse_subdistrict,
                          meta={"district_name": district_name, "category": self.category})

    def parse_subdistrict(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        ### 得到子区域列表
        subdistrict_urls = []
        # 5i5j
        if not subdistrict_urls:
            self.logger.info("5a5j subdistrict ...")
            subdistrict_urls = response.xpath('//dd[@class="block"]//a')
        # lainjia
        if not subdistrict_urls:
            self.logger.info("lianjian subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="option-list sub-option-list"]//a[not(text()="不限")]')
        #anjuke
        if not subdistrict_urls:
            self.logger.info("anjuke subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="全部"])[2]//ancestor::div[@class="sub-items"]//a[not(.//text()="全部")]')
        # qfang
        if not subdistrict_urls:
            self.logger.info("qfang subdsitrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::ul//a[not(text()="不限")]')
        # ganji
        if not subdistrict_urls:
            self.logger.info("ganji subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]//a[not(text()="不限")]')
        if not subdistrict_urls:
            self.logger.info("fang subdistrict ...")
            subdistrict_urls = response.xpath('//p[@id="shangQuancontain"]//a[not(text()="不限")]')
        if not subdistrict_urls:
            self.logger.info("centanet subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::p[@class="subterm fl"]//a[not(text()="不限")]')
        ###
        ##
        district = response.meta.get("district_name")
        category = response.meta.get("category")

        ### 若子区域列表为空 插入一条subdistrict 为nodef的数据.
        if not subdistrict_urls:
            self.logger.critical("!!!! url: <%s> not  found any sub_districts, checkout again  !!!!", response.url)
            l = ItemLoader(item=DistrictItem())
            l.default_output_processor = TakeFirst()
            l.add_value("district", district)
            l.add_value("subdistrict", "nodef")
            l.add_value("url", response.url)
            l.add_value("category", category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

        for url in subdistrict_urls:
            subdistrict_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            subdistrict = "".join(url.xpath('.//text()').extract()).strip()

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("district", district)
            l.add_value("subdistrict", subdistrict)
            l.add_value("url", subdistrict_url)
            l.add_value("category", category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()
            meta = {"district":district,"subdistrict":subdistrict,"category":category}


class AgencyIndexPageSpider(scrapy.spiders.CrawlSpider):
    name = "AgencyIndexPageSpider"

    xpaths = ['//span[text()="下一页 >"]//ancestor::a[1]', # ganji
                "//a[text()=">"]",                              # centanet
                 '//a[text()="下一页"]',                        # 5i5j, fang
                '//div[@class="page-box house-lst-page-box"]//a', # lianjia
                '//a[text()="下一页 >"]',                         # anjuke
                '//span[text()="下一页"]//ancestor::a[1]' # qfang
              ]
    rules = (
        # ganji
        Rule(LinkExtractor(restrict_xpaths=xpaths),
             callback='parse_indexpage',follow=True),

    )

    def start_requests(self):
        with sqlite3.connect(get_project_settings().get("STORE_DATABASE")) as cnx:
            cursor = cnx.cursor()
            cursor.execute("select district,subdistrict,url from main.district where instr(source, '.5i5j.com') > 0")
            url_infos = cursor.fetchall()

        for url_info in url_infos:
            meta = {"district":url_info[0],"subdistrict":url_info[1]}
            yield Request(url=url_info[2],meta=meta)

    def parse_start_url(self, response):
        for item in self.parse_indexpage(response):
            yield item

    def parse_indexpage(self,response):
        self.logger.info("process url: <%s>", response.url)
        items = []

        if ".lianjia.com" in response.url:
            items = self.parse_lianjia(response)
        elif ".anjuke.com" in response.url:
            items = self.parse_anjuke(response)
        elif ".qfang.com" in response.url:
            items = self.parse_qfang(response)
        elif ".centanet.com" in response.url:
            items = self.parse_centanet(response)
        elif ".ganji.com" in response.url:
            items = self.parse_fang(response)
        elif ".fang.com" in response.url:
            items = self.parse_ganji(response)
        elif ".5i5j.com" in response.url:
            items = self.parse_5i5j(response)
        else:
            self.logger.critical("not parse find")

        for item in items:
            yield item

    def parse_lianjia(self, response):
        self.logger.info("process lianjia url")
        ul = response.xpath('//ul[@class="agent-lst"]/li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/div[@class="agent-name"]//h2/text()')
            l.add_xpath("district",'.//div[@class="main-plate"]//a[1]/text()', MapCompose(lambda x: x.strip()))
            l.add_xpath("subdistrict",'.//div[@class="main-plate"]//a[2]/text()', MapCompose(lambda x: x.strip()))
            l.add_xpath("telephone",'.//p[@class="mobile_p"]/text()')
            l.add_xpath("history_amount",'.//span[@class="LOGCLICKEVTID"]/text()',
                        MapCompose(lambda x: int(x)), re = r"\d+")
            l.add_xpath("recent_activation", './/div[@class="achievement"]/span/text()',
                        MapCompose(lambda x: int(x)), re = r"(\d+)套")

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_anjuke(self,response):
        self.logger.info("process anjuke url")
        divs = response.xpath('//div[@class="jjr-itemmod"]')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name",".//h3/a/text()", Join())
            l.add_xpath("company", './/p[@class="jjr-desc"]/a[1]/text()')
            l.add_xpath("address", './/p[@class="jjr-desc"]/a[2]/text()')
            l.add_xpath("telephone", './/div[@class="jjr-side"]/text()'
                        ,MapCompose(lambda x: int(x)),re = r"\d+")
            l.add_xpath("district",'(//span[@class="elems-l"]//a[@class="selected-item"])[1]//text()')
            l.add_xpath("subdistrict",'(//span[@class="elems-l"]//a[@class="selected-item"])[2]//text()')

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_qfang(self, response):
        self.logger.info("process qfang url")
        ul = response.xpath('//div[@id="find_broker_lists"]//li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name",'//p[@class="name fl"]//a/text()')
            l.add_xpath("district", './/span[@class="con fl"]/b[1]/text()')
            l.add_xpath("subdistrict",'.//span[@class="con fl"]/b[2]/text()')
            l.add_xpath("telephone", './/div[@class="broker-tel fr"]/p/text()',
                        MapCompose(lambda x: int(x)), re = r"\d+")
            l.add_xpath("history_amount", './/span[@class="con fl"]/em/text()')

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_centanet(self, response):
        self.logger.info("process centanet url")
        ul = response.xpath('//ul[@class="broker_list broker_listSZ"]/li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/p[@class="phone"]/b/@zvalue',
                        re=r"cnName:'(\w+)'")
            l.add_xpath("district",'.//h2//@title')
            l.add_xpath("subdistrict", './/p[@class="xi"]//@title',
                        Join('-'))
            l.add_xpath("telephone",'.//p[@class="phone"]/b/@zvalue'
                        ,re = r"mobile:'(\w+)'")
            l.add_xpath("second_house_amount",'.//div[@class="outstanding"]//p[1]/a/text()'
                        ,re = r"\d+")
            l.add_xpath("rent_house_amount", './/div[@class="outstanding"]//p[2]/a/text()'
                        , re=r"\d+")
            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_ganji(self, response):
        self.logger.info("process ganji url")
        divs = response.xpath('//div[@class="f-list-item"]')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/a[@class="broker-name"]/text()')
            l.add_xpath("district", './/span[@class="bi-text"]/a[1]/text()')
            l.add_xpath("subdistrict", './/span[@class="bi-text"]/a[2]/text()')
            l.add_xpath("telephone", './/p[@class="tel"]/text()', MapCompose(lambda x: int(x)))

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_fang(self, response):
        self.logger.info("process fang url")
        ul = response.xpath('//li[@link]')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/div[@class="ttop"]//a//text()')
            l.add_xpath("district", './/span[@class="diqu"]/span[1]/text()')
            l.add_xpath("subdistrict", './/span[@class="diqu"]/span[2]/text()')
            l.add_xpath("telephone", './/div[@class="fl"]/p[1]/text()',
                        MapCompose(lambda x: int(x)), re=r"\d+")
            l.add_xpath("company", '//li[@link]//p[@class="f14 liaxni"]/span[2]/text()',Join(','),
                        re=r"\w+")

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()


    def parse_5i5j(self,response):
        self.logger.info("5i5j fang url")
        divs = response.xpath('//div[@class="list-con-box"]/div')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name",'.//h3/text()')
            l.add_xpath("district", '//li[@class="new_di_tab_cur"]//text()',
                        MapCompose(lambda x:x.strip()), Join())
            l.add_xpath("subdistrict", '//dd//a[@class="cur"]//text()',
                        MapCompose(lambda x: x.strip()), Join())
            l.add_xpath("address", './/p[@class="iconsleft"]//text()',Join())
            l.add_xpath("telephone",'.//div[@class="contacty"]/span/text()',
                        MapCompose(lambda x: int(x)), re='\d+')
            l.add_xpath("recent_activation", './/p[@class="eye-icons"]',
                        MapCompose(lambda x: int(x)), re = '(\d+)次')
            l.add_xpath("history_amount", './/p[@class="iconsleft1"]/text()',
                         MapCompose(lambda x: int(x)), re ='买卖(\d+)')
            l.add_xpath("rent_house_amount", './/p[@class="iconsleft1"]/text()',
                         MapCompose(lambda x: int(x)), re ='租赁(\d+)')


            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()


class TestCrawlSpider(scrapy.spiders.CrawlSpider):
    name = "TestCrawlSpider"
    xpaths = ['//a[text()="下一页"]','//div[@class="page-box house-lst-page-box"]//a']  # 5i5j, fang

    rules = (
        # ganji
        Rule(LinkExtractor(restrict_xpaths=xpaths),
             callback='parse_indexpage', follow=True),
    )

    def start_requests(self):
      yield Request(url = 'https://sh.5i5j.com/jingjiren/caoyang/')

    def parse_start_url(self, response):
        for item in self.parse_indexpage(response):
            yield item

    def parse_indexpage(self, response):
        for i in range(2):
            yield  i
