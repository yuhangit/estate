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


class FangDDScrapeSpider(scrapy.Spider):
    name = 'FangDDScrapeSpider'
    index_name = 'FangDDIndexSpider'
    spc_reg = re.compile(r"\s+")
    allowed_domains = ['esf.sh.fang.com/']

    def __init__(self):
        super(self.__class__,self).__init__()
        self.cnx = sqlite3.connect(get_project_settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.cursor.execute("PRAGMA JOURNAL_MODE =WAL ")
        self.start_urls = self._get_urls()

    def _get_urls(self):
        self.cursor.execute("select url from index_pages where retrived = 0 and spider = '%s' limit 100" % self.index_name)
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

        district = response.xpath('(//div[@class="_23XzT"]//text())[1]').extract_first().strip().replace("\"", "")
        subdistrict = response.xpath('(//div[@class="_23XzT"]//text())[2]').extract_first().strip().replace("\"", "")

        for div in response.xpath('//ul[@class=""]/li'):
            l = ItemLoader(item=ScrapeItem(), selector=div)
            l.default_output_processor = TakeFirst()
<<<<<<< HEAD
            l.add_xpath("title",'(.//a)[1]//text()', MapCompose(lambda x: self.spc_reg.sub("",x)))
            l.add_xpath("url","(.//a)[1]//(@href",
=======
            l.add_xpath("title",'(.//a)[1]/text()', MapCompose(lambda x: self.spc_reg.sub("",x)))
            l.add_xpath("url","(.//a)[1]/@href",
>>>>>>> 4246b612227e686f1eee6e18836aebfbbe36414c
                        MapCompose(lambda x: urljoin(response.url,urlparse(x).path)))
            l.add_xpath("price", './/span[text() = "万"]/..//text()', Join())
            l.add_xpath("address",'.//span[@class="_13KXy"]//text()',
                        MapCompose(lambda x: self.spc_reg.sub("",x)),Join('-'))
            l.add_value("district",district)
            l.add_value("subdistrict",subdistrict)

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

            yield l.load_item()
        self._upd_retrived(response.url, 1)
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


class FangIndexSpider(scrapy.Spider):
    name = "FangDDIndexSpider"
    start_urls = ["http://shanghai.fangdd.com/esf/"]
    # rules = (
    #     Rule(LinkExtractor(restrict_xpaths='//a[text() =">"]'),
    #          callback='parse_item', follow=False),)
    page_num_reg = re.compile(r"\d{1,2}/$")

    def parse(self, response):
        dist_urls = response.xpath('(//a[text() = "不限"])[1]/ancestor::ul//a[not(text() = "不限")]/@href').extract()

        for dist_url in dist_urls:
            url = response.urljoin(dist_url)
            yield Request(url, callback=self.get_subdist_urls)

    def get_subdist_urls(self, response):

        subdist_urls = response.xpath('(//a[text() = "不限"])[2]/ancestor::ul//a[not(text() = "不限")]/@href').extract()
        for subdist_url in subdist_urls:
            url = response.urljoin(subdist_url)
            yield Request(url,callback=self.parse_item)

    def parse_item(self, response):
        self.logger.info("starting parse item")

        last_page = response.xpath('//div[@class="_39bCK"]//a[contains(@data-analytics-track-event,"event")][last()]/@href').extract_first()
        if last_page:
            print('-'*12,self.page_num_reg.findall(last_page) ,'-'*12)
            last_page_num = int(self.page_num_reg.findall(last_page)[0][:-1])

            for i in range(1,last_page_num+1):

                l = ItemLoader(item=IndexItem())
                l.default_output_processor = TakeFirst()
                l.add_value("url", response.urljoin(self.page_num_reg.sub(r'%s/' %i, last_page)))
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


