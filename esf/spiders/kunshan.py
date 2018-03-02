# -*- coding: utf-8 -*-
import scrapy
from esf.items import PropertyItem,IndexItem,AgentItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose,Join,TakeFirst
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
import sqlite3
from urllib.parse import urlparse,parse_qs
import re
import socket
import datetime
import pymysql
import dj_database_url



class KunshanAllScrapeScripe(scrapy.spiders.CrawlSpider):
    start_urls = [ 'http://house.ks.js.cn/secondhand.asp']
    name = 'KunShanAllScrapeSpider'
    spc_reg = re.compile(r"\s+")

    city_name = "上海周边"
    dist_name = "昆山"
    category = "二手房"
    station_name = "昆山视窗"

    rules = (Rule(LinkExtractor(restrict_xpaths='//div[@class="page"]')),
             Rule(LinkExtractor(restrict_xpaths='//ul[@id="xylist"]/li//a'), callback="parse_item")
             )

    def parse_item(self,response):
        # agency table
        l = ItemLoader(item=AgentItem(), response=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("name", '//div[@class="sthys3"]/text()', re=r"：(\w+)")
        l.add_xpath("telephone", '//div[@class="sttelct2 sttelct"]/text()',
                    MapCompose(lambda x: "".join(x.split())))
        l.item.setdefault("company", None)
        l.add_xpath("company", '//li[@class="st14 stb starial"]//text()')
        l.add_xpath("address", '//div[@class="xflilist"]/div[3]//text()',
                    re = r'：(\w+)')
        l.add_xpath("register_date", '//div[@class="jbfx"]/text()', re=r'登记日期：([\d/]+)')

        l.add_value("city_name", self.city_name)
        l.add_value("dist_name", self.dist_name)
        l.add_value("category", self.category)
        l.add_value("station_name", self.station_name)
        l.add_xpath("subdist_name", '(//div[@class="xx_xq_l200"])[2]/text()', re='区域：(?:昆山)?(\\w+)')

        if not l.item.get("subdist_name"):
            self.logger.critical("subdsitrict name is not scrape, save response as a file")
            f = open("html_%s.html", parse_qs(urlparse(response.url).query).get("id")[0])
            f.write(response.url)
            f.close()

        # housekeeping
        l.add_value("source", response.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("date", datetime.datetime.utcnow())
        yield l.load_item()

        # properties table
        l = ItemLoader(item=PropertyItem(), response=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath('title', '//div[@class="xxview_title"]/text()')
        l.add_value("url", response.url)
        l.add_xpath("price", '//div[@class="xx_xq_l200"]/span[@class="st22 '
                             'sthuangs stb starial"]/text()')
        l.add_xpath("address",'//div[@class="wydzleft"]/text()', MapCompose(lambda x: x.strip()),
                    re=r'物业地址：([^\x01-\x1f]+)')
        l.add_xpath("agent_name", '//div[@class="sthys3"]/text()', re=r"：(\w+)")
        l.item.setdefault("agent_company", None)
        l.add_xpath("agent_company", '//li[@class="st14 stb starial"]//text()')
        l.add_xpath('agent_phone','//div[@class="sttelct2 sttelct"]/text()',
                    MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("recent_activation", '//div[@class="fyfbtime"]/text()', re = '查看人次：(\\d+)')

        l.add_value("city_name", self.city_name)
        l.add_value("dist_name", self.dist_name)
        l.add_value('station_name', self.station_name)
        l.add_value("category", self.category)
        l.add_xpath("subdist_name", '(//div[@class="xx_xq_l200"])[2]/text()', re='区域：(?:昆山)?(\\w+)')

        # housekeeping
        l.add_value("source", response.request.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("date", datetime.datetime.utcnow())
        yield l.load_item()

    def start_requests(self):

        self.logger.critical("refresh urls ....")
        db_para = self.__class__.parse_mysql_url(get_project_settings().get("MYSQL_PIPELINE_URL"))
        # 刷新被服务器防火墙屏蔽的网页, SkipExistUrl
        cnx = pymysql.connect(**db_para, charset="utf8")
        with cnx.cursor() as cursor:

            cnt = cursor.execute("""DELETE from estate.agencies_temp 
                                    where name is null and district_id in 
                                        (select district_id 
                                        from district_rel 
                                        where dist_name = %s)""", (self.dist_name,))

            self.logger.info("delete %s from estate.agencies where name is null and dist_name = '%s'", cnt, self.dist_name,)
            cnt = cursor.execute("""DELETE from estate.properties_temp 
                                    where agent_name is null and station_id in 
                                      (select station_id from station_rel where station_name = %s) """, (self.station_name, ))
            self.logger.info("delete %s from estate.properties station_name = '%s' and name is NULL ", cnt, self.station_name)
            cnx.commit()

        for url in self.start_urls:
            yield Request(url=url)

    @staticmethod
    def parse_mysql_url(mysql_url):
        params = dj_database_url.parse(mysql_url)
        conn_kwargs = {}
        conn_kwargs["host"] = params["HOST"]
        conn_kwargs["user"] = params["USER"]
        conn_kwargs["passwd"] = params["PASSWORD"]
        conn_kwargs["db"] = params["NAME"]
        conn_kwargs["port"] = params["PORT"]
        # remove items with empty values
        conn_kwargs = dict((k, v) for k, v in conn_kwargs.items() if v)
        return conn_kwargs
    # def parse(self, response):
    #     self.logger.info("start parese url %s" %response.url)
    #     for div in response.xpath('//ul[@id="xylist"]/li[@class="listzwt"]'):
    #         l = ItemLoader(item=PropertyItem(), selector=div)
    #         l.default_output_processor = TakeFirst()
    #         l.add_xpath("title",'./div[@class="xlist_1"]/a/text()', MapCompose(lambda x: self.spc_reg.sub("",x)), Join())
    #         l.add_xpath("url",'./div[@class="xlist_1"]/a/@href',
    #                     MapCompose(lambda x: urljoin(response.url, x )))
    #         l.add_xpath("price", '(./div[@class="xlist_3"])[3]/text()')
    #         l.add_xpath("address",'./div[@class="xlist_1"]/a/text()',
    #                     MapCompose(lambda x: self.spc_reg.sub("", x)),Join())
    #
    #         l.add_value("dist_name", "昆山")
    #
    #         l.add_xpath("subdist_name",'./div[@class="xlist_2"]/text()')
    #
    #         # housekeeping
    #         l.add_value("source", response.url)
    #         l.add_value("project", self.settings.get("BOT_NAME"))
    #         l.add_value("spider", self.name)
    #         l.add_value("server", socket.gethostname())
    #         l.add_value("date", datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    #
    #         yield l.load_item()

    # def start_requests(self):
    #     self.cnx = sqlite3.connect(get_project_settings().get("STORE_DATABASE"))
    #     self.cursor = self.cnx.cursor()
    #     self.cursor.execute("SELECT DISTINCT url from properties where spider = '%s'" %self.name)
    #     fetched_urls = [url[0] for url in self.cursor.fetchall()]
    #     for url in self.start_urls:
    #         if url not in fetched_urls:
    #             yield Request(url)

    # def __del__(self):
    #     self.cursor.close()
    #     self.cnx.close()
