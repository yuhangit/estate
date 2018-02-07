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



class KunshanAllScrapeScripe(scrapy.Spider):
    start_urls = [ 'http://house.ks.js.cn/secondhand.asp?page=%s&wn=&g=&w=&s=0&j=&x=&q=&l=&regid='
                   %i for i in range(1,2963)]
    name = 'KunShanAllScrapeSpider'
    spc_reg = re.compile(r"\s+")

    def parse(self, response):
        self.logger.info("start parese url %s" %response.url)
        for div in response.xpath('//ul[@id="xylist"]/li[@class="listzwt"]'):
            l = ItemLoader(item=ScrapeItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("title",'./div[@class="xlist_1"]/a/text()', MapCompose(lambda x: self.spc_reg.sub("",x)), Join())
            l.add_xpath("url",'./div[@class="xlist_1"]/a/@href',
                        MapCompose(lambda x: urljoin(response.url, x )))
            l.add_xpath("price", '(./div[@class="xlist_3"])[3]/text()')
            l.add_xpath("address",'./div[@class="xlist_1"]/a/text()',
                        MapCompose(lambda x: self.spc_reg.sub("", x)),Join())

            l.add_value("district", "昆山")

            l.add_xpath("subdistrict",'./div[@class="xlist_2"]/text()')

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
