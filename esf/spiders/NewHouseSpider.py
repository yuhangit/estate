# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem,IndexItem,PropertyItem
from scrapy.loader import ItemLoader
from scrapehelper import DBConnect, ScrapeHelper
from urllib.parse import urlparse, urlencode
import socket
import datetime
import sqlite3
import re


# cannot use CrawlSpider for lack pass meta between rules
class NewHouseDistrictSpider(scrapy.Spider, ScrapeHelper):
    name = "NewHouseDistrictSpider"

    category = "新房"
    dist_xpath = [
        ['//div[@class="termline clearfix"][1]/p[@class="termcon fl"]/a[not(text()="不限")]', "中原地产"],
         ['//li[@id="quyu_name"]/a[not(text()="不限")]', '房天下'],
        ['//ul[@class="f-clear"]//a[not(text()="不限")]',  '赶集网'],
        ['//ul[@class="_3p3k4 _3MnR2"]//a[not(text()="不限")]', '房多多'],
        ['//ul[@class="search-area-detail clearfix"]//a[not(text()="不限")]', 'Q房网'],
    ]

    def start_requests(self):
        start_urls = get_project_settings().get("CATEGORIES")[self.category]
        for url,meta in start_urls.items():
            yield Request(url=url, meta=meta)

    def parse(self, response):
        district = []

        info = None
        for xpath in self.dist_xpath:
            if not district:
                district = response.xpath(xpath[0])
                info = xpath[1]
            else:
                self.logger.info("find dist_name in [%s]", info)
                break

        city_name = response.meta.get("city_name")
        station_name = response.meta.get("station_name")
        category = response.meta.get("category")

        if not district:
            self.logger.error("!!!! url: %s not found any districts, checkout again this  !!!!", response.url)
            l = ItemLoader(item=IndexItem())
            l.default_output_processor = TakeFirst()
            l.add_value("url", response.url)

            l.add_value("dist_name", None)
            l.add_value("subdist_name", None)
            l.add_value("category", self.category)
            l.add_value("city_name", city_name)
            l.add_value("station_name", station_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()

        meta = self.get_meta_info(response.meta)

    def parse_subdistrict(self, response):
        subdistrict = []


        if not subdistrict:
            self.logger.info("centanet url ...")
            subdistrict = response.xpath('//p[@id="PanelBlock"]//a[not(text()="不限")]')
            dist_name = response.xpath('(//p[@class="termcon fl"])[1]//a[@class="curr"]/text()').extract_first()

        if not subdistrict:
            self.logger.info("fang url ...")
            subdistrict = response.xpath('//li[@id and @_fckxhtmljob="1"]/a')
            dist_name = response.xpath('(//a[@class="hui" and @href="#no"])[1]/text()').extract_first()

        if not subdistrict:
            self.logger.info("ganji url ...")
            subdistrict = response.xpath('//div[@class="fou-list f-clear"]//a[not(text()="不限")]')
            dist_name = response.xpath('//div[@class="thr-list"]//li[@class="item current"]/a/text()').extract_first()

        if not subdistrict:
            self.logger.info("fangdd url ...")
            subdistrict = response.xpath('//ul[@class="_3p3k4 _2yGcr"]//a[not(text()="不限")]')
            dist_name = response.xpath('//li[@class="aaK37 rwyU0 _2xI7o"]/a/text()').extract_first()

        if not subdistrict:
            self.logger.info("Qfang url ...")
            subdistrict = response.xpath('//ul[@class="search-area-second clearfix"]//a[not(text()="不限")]')
            dist_name = response.xpath('//ul[@class="search-area-detail clearfix"]//a[@class="current"]/text()').extract_first()

            if dist_name == "周边城市":
                city_name = "上海周边"
                dist_name = "不限"

        # handle exception
        if not subdistrict:
            self.logger.critical("!!!! url: <%s> not  found any sub_districts, checkout again  !!!!", response.url)
            l = ItemLoader(item=IndexItem())
            l.default_output_processor = TakeFirst()

            l.add_value("url", response.url)

            l.add_value("dist_name", dist_name)
            l.add_value("subdist_name", None)
            l.add_value("category", category)
            l.add_value("city_name", city_name)
            l.add_value("station_name", station_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()



