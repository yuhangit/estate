# -*- coding: utf-8 -*-
import scrapy
from esf.items import PropertyItem,IndexItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose,Join,TakeFirst
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import Request
from scrapy.utils.project import get_project_settings as settings
import sqlite3
import re
import socket
import datetime
from urllib.request import urljoin,urlparse
import random
import time

import logging


class GanjiSpider(CrawlSpider):
    name = 'ganji'
    spc_reg = re.compile("\\s+")
    allowed_domains = ['sh.ganji.com']
    start_urls = ['http://sh.ganji.com/fang5/']
    logger = logging.getLogger(__file__)
    rules = (
        Rule(LinkExtractor(restrict_xpaths='//a/span[contains(text(),"下一页")]/..'),
             callback='parse_item', follow=True),
    )

    def __init__(self):
        super(GanjiSpider,self).__init__()
        self.cnx = sqlite3.connect(settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.start_urls = self._get_urls()

    def _get_urls(self):
        self.cursor.execute("select url from index_pages where retrived = 0")
        ls = [row[0] for row in self.cursor.fetchall()]
        return ls

    def _upd_retrived(self,url,value):
        self.logger.info("update url : %s" %url)
        self.cursor.execute("update index_pages  set retrived = ? where url = ?",[value, url])
        self.cnx.commit()

    def __del__(self):
        self.cursor.close()
        self.cnx.close()

    def parse_item(self, response):
        district = response.xpath("(//a[text()='不限'])[1]//ancestor::ul//li[@class='item current']//text()").extract_first()
        subdistrict = response.xpath("(//a[text()='不限'])[2]//ancestor::div//a[@class='subway-item current']//text()").extract_first()

        for div in response.xpath("//div[contains(@id,'puid-')]"):
            l = ItemLoader(item=PropertyItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("title","(.//a)[2]/text()", MapCompose(lambda x: self.spc_reg.sub("",x)))
            l.add_xpath("url","(.//a)[2]/@href",
                        MapCompose(lambda x: urljoin(response.url,urlparse(x).path)))
            l.add_xpath("price", ".//div[@class='price']//text()",Join())
            l.add_xpath("address",".//span[@class='area']//text()",
                        MapCompose(lambda x: self.spc_reg.sub("",x)),Join())
            l.add_value("dist_name",district)
            l.add_value("subdist_name",subdistrict)

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()
        self._upd_retrived(response.url,1)
        #i['domain_id'] = response.xpath('//input[@id="sid"]/@value').extract()
        #i['name'] = response.xpath('//div[@id="name"]').extract()
        #i['description'] = response.xpath('//div[@id="description"]').extract()

    def parse_start_url(self, response):
        print("start prase start url..."*5)
        self.logger.info("start ")

        for item in self.parse_item(response):
            yield item


class PageSpider(GanjiSpider):
    name = "indexpage"

    def parse_item(self, response):
        self.logger.info("starting parse item")
        l = ItemLoader(item=IndexItem(),response=response)
        l.default_output_processor = TakeFirst()
        l.add_value("url", response.url)
        l.add_value("retrived",0)

        l.add_value("source", response.request.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("date", datetime.datetime.utcnow())

        yield l.load_item()

    def _get_urls(self):
        self.cursor.execute("select url from lvl1_urls where retrived = 0")
        ls = [row[0] for row in self.cursor.fetchall()]
        return ls

class PageScrape(scrapy.Spider):
    name = "scrapepage"
    spc_reg = re.compile(r"\s+")

    def parse(self, response):
        district = response.xpath(
            "(//a[text()='不限'])[1]//ancestor::ul//li[@class='item current']//text()").extract_first()
        subdistrict = response.xpath(
            "(//a[text()='不限'])[2]//ancestor::div//a[@class='subway-item current']//text()").extract_first()

        for div in response.xpath("//div[contains(@id,'puid-')]"):
            l = ItemLoader(item=PropertyItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("title", "(.//a)[2]/text()", MapCompose(lambda x: self.spc_reg.sub("", x)))
            l.add_xpath("url", "(.//a)[2]/@href",
                        MapCompose(lambda x: urljoin(response.url, urlparse(x).path)))
            l.add_xpath("price", ".//div[@class='price']//text()", Join())
            l.add_xpath("address", ".//span[@class='area']//text()",
                        MapCompose(lambda x: self.spc_reg.sub("", x)), Join())
            l.add_value("dist_name", district)
            l.add_value("subdist_name", subdistrict)

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()
        self._upd_retrived(response.url, 1)

    def __init__(self):
        super(PageScrape,self).__init__()
        self.cnx = sqlite3.connect("scrapy_ganji_esf_urls.db")
        self.cursor = self.cnx.cursor()
        self.start_urls = self._get_urls()

    def _get_urls(self):
        self.cursor.execute("select url from index_pages where retrived = 0 " )
        ls = [row[0] for row in self.cursor.fetchall()]
        return ls

    def _upd_retrived(self,url,value):
        self.logger.info("update url : %s" %url)
        self.cursor.execute("update index_pages  set retrived = ? where url = ?",[value, url])
        self.cnx.commit()

    def __del__(self):
        self.cursor.close()
        self.cnx.close()