# -*- coding: utf-8 -*-
import scrapy
from esf.items import ScrapeItem,IndexItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose,Join,TakeFirst
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
import sqlite3
import re
import socket
import datetime
from urllib.request import urljoin, urlparse
import random
import time
import logging
from bs4 import BeautifulSoup
import requests


class CentanetScrapeSpider(scrapy.Spider):
    name = 'CentanetScrapeSpider'
    index_name = 'CentanetIndexSpider'
    spc_reg = re.compile(r"\s+")
    allowed_domains = ['sh.centanet.com']

    def __init__(self):
        super(self.__class__,self).__init__()
        self.cnx = sqlite3.connect(get_project_settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.start_urls = self._get_urls()

    def _get_urls(self):
        self.cursor.execute("select url from index_pages where retrived = 0 and spider = '%s'" % self.index_name)
        ls = [row[0] for row in self.cursor.fetchall()]
        return ls

    def _upd_retrived(self,url,value):
        self.logger.info("update url %s" %url)
        self.cursor.execute("update index_pages set retrived = ? where url = ?",[value, url])
        self.cnx.commit()

    def __del__(self):
        self.cursor.close()
        self.cnx.close()

    def parse(self, response):

        district = response.xpath('(//span[@class="curr"])[1]/text()').extract_first()
        subdistrict = response.xpath('(//span[@class="curr"])[2]/text()').extract_first()

        for div in response.xpath('//div[@class="house-listBox"]/div'):
            l = ItemLoader(item=ScrapeItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("title",'(.//a)[2]/text()', MapCompose(lambda x: self.spc_reg.sub("",x)))
            l.add_xpath("url","(.//a)[2]/@href",
                        MapCompose(lambda x: urljoin(response.url,urlparse(x).path)))
            l.add_xpath("price", './/p[@class="price-nub cRed"]/text()',Join())
            l.add_xpath("address",'.//a[@class="f000 mr_10"]//text()',
                        MapCompose(lambda x: self.spc_reg.sub("",x)),Join())
            l.add_value("district",district)
            l.add_value("subdistrict",subdistrict)

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

            yield l.load_item()
        self._upd_retrived(response.url,1)
        #i['domain_id'] = response.xpath('//input[@id="sid"]/@value').extract()
        #i['name'] = response.xpath('//div[@id="name"]').extract()
        #i['description'] = response.xpath('//div[@id="description"]').extract()

    def _init_urls(self):
        cnx = sqlite3.connect(get_project_settings().get("STORE_DATABASE"))
        cursor = cnx.cursor()
        cursor.execute("""CREATE TABLE if NOT EXISTS  lvl1_urls
                            (id  INTEGER PRIMARY KEY  AUTOINCREMENT,url TEXT,
                               dt DATE DEFAULT current_time,retrived INTEGER DEFAULT 0)
                        """)
        cursor.execute("""CREATE TABLE lvl0_urls(id INTEGER PRIMARY KEY   AUTOINCREMENT,
                            url TEXT, dt DATE DEFAULT current_time,retrived INTEGER DEFAULT 0
                        )""")

        headers = get_project_settings()["REQUESTS_HEADERS"]
        session = requests.session()
        session.headers = headers


class CentanetIndexSpider(scrapy.Spider):
    name = "CentanetIndexSpider"
    start_urls = ["http://sh.centanet.com/ershoufang/"]
    # rules = (
    #     Rule(LinkExtractor(restrict_xpaths='//a[text() =">"]'),
    #          callback='parse_item', follow=False),)
    page_num_reg = re.compile(r'/([a-zA-z_-]*)(\d+)/$')

    def parse(self, response):
        dist_urls = response.xpath('(//span[text() = "不限"])[1]/..//a[not (text() = "不限")]/@href').extract()

        for dist_url in dist_urls:
            url = response.urljoin(dist_url)
            yield Request(url, callback=self.get_subdist_urls)

    def get_subdist_urls(self, response):

        subdist_urls = response.xpath('(//span[text() = "不限"])[1]/..//a[not (text() = "不限")]/@href').extract()
        for subdist_url in subdist_urls:
            url = response.urljoin(subdist_url)
            yield Request(url,callback=self.parse_item)

    def parse_item(self, response):
        self.logger.info("starting parse item")
        last_page = response.xpath('//a[text() =">>"]/@href').extract_first()
        if last_page:
            print('-'*12,self.page_num_reg.findall(last_page) ,'-'*12)
            last_page_num = int(self.page_num_reg.findall(last_page)[0][1])

            for i in range(1,last_page_num+1):

                l = ItemLoader(item=IndexItem())
                l.default_output_processor = TakeFirst()
                l.add_value("url", response.urljoin(self.page_num_reg.sub(r'/\g<1>%s/' %i, last_page)))
                l.add_value("retrived", 0)

                l.add_value("source", response.request.url)
                l.add_value("project", self.settings.get("BOT_NAME"))
                l.add_value("spider", self.name)
                l.add_value("server", socket.gethostname())
                l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                yield l.load_item()
        else:
            l = ItemLoader(item=IndexItem())
            l.default_output_processor = TakeFirst()
            l.add_value("url", response.urljoin(response.url))
            l.add_value("retrived", 0)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()


class CentanetAllScrapeScripe(scrapy.Spider):
    start_urls = [ 'http://sh.centanet.com/ershoufang/g%s/' %i for i in range(1,1607)]
    name = 'CentanetAllScrapeSpider'
    spc_reg = re.compile(r"\s+")

    def parse(self, response):
        self.logger.info("start parese url %s" %response.url)
        for div in response.xpath('//div[@class="house-listBox"]/div'):
            l = ItemLoader(item=ScrapeItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("title", '(.//a)[2]/text()', MapCompose(lambda x: self.spc_reg.sub("",x)))
            l.add_xpath("url", "(.//a)[2]/@href",
                        MapCompose(lambda x: urljoin(response.url,urlparse(x).path)))
            l.add_xpath("price", './/p[@class="price-nub cRed"]/text()',Join())
            l.add_xpath("address",'.//a[@class="f000 mr_10"]//text()',
                        MapCompose(lambda x: self.spc_reg.sub("",x)),Join())

            l.add_xpath("district", './/p[@class="f7b mb_15"]/text()',Join(), MapCompose(lambda x: x.split("-")[0].strip()))

            l.add_xpath("subdistrict",'.//p[@class="f7b mb_15"]/text()',Join(), MapCompose(lambda x: x.split("-")[1].split()[0]))

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

            yield l.load_item()

    def start_requests(self):
        self.cnx = sqlite3.connect(get_project_settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.cursor.execute("SELECT DISTINCT url from properties where spider = '%s'" %self.name)
        fetched_urls = [url[0] for url in self.cursor.fetchall()]
        for url in self.start_urls:
            if url not in fetched_urls:
                yield Request(url)

    def __del__(self):
        self.cursor.close()
        self.cnx.close()


class CentanetAgentAllScrapeScripe(scrapy.Spider):
    start_urls = [ 'http://sh.centanet.com/jingjiren/g%s/' %i for i in range(1,340)]
    name = "CentanetAgentSpider"

    def parse(self, response):
        itemlists = response.xpath('//li[@class="clearfix js_point_list"]')
        logging.critical("--"*12+"    "+"--"*12)
        logging.critical("url: %s, items: %s" % (response.url, len(itemlists)))
        i = 0
        for items in itemlists:
            store = items.xpath('./@zvalue').re_first(r"name:'(.*)'")
            para = items.xpath('./@zvalue').re_first(r"para:'([^']*)'")
            url = response.url
            mobile = items.xpath('.//p[@class="phone"]/b/@zvalue').re_first(r"mobile:'(.*)'")
            name = items.xpath('.//p[@class="phone"]/b/@zvalue').re_first(r"cnName:'([^,]*)'")
            secondHouse = Join()(items.xpath('(.//div[@class="outstanding"]/p)[1]//text()').re(r"(.*)"))
            rentHouse = Join()(items.xpath('(.//div[@class="outstanding"]/p)[2]//text()').re(r"(.*)"))
            visitedCount = Join()(items.xpath('(.//div[@class="outstanding"]/p)[3]//text()').re(r"(.*)"))
            i += 1
            yield {
                "url":url, "store":store, "para":para, "mobile":mobile,
                "name":name, "secondHouse": secondHouse, "rentHouse": rentHouse,
                "visitedCount": visitedCount
            }
        logging.critical("count: %s" % i)

