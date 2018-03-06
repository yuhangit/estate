# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem,IndexItem,PropertyItem
from scrapy.loader import ItemLoader
from scrapehelper import ScrapeHelper, DBConnect
from urllib.parse import urlparse,urlencode
import socket
import datetime
import sqlite3
import re


class SecondHouseDistrictSpider(scrapy.Spider, ScrapeHelper):
    name = "SecondHouseDistrictSpider"
    category = "二手房"

    def start_requests(self):
        start_urls = get_project_settings().get("CATEGORIES")[self.category]
        for url, meta in start_urls.items():
            yield Request(url=url, meta=meta)

    def parse(self, response):
        district = []

        # centanet
        if not district:
            district = response.xpath('(//span[text()="不限"])[1]/ancestor::p//a[not(text()="不限")]')
        # fang
        if not district:
            district = response.xpath('(//a[text()="不限"])[1]/ancestor::div[@id="list_D02_10"]//a[not(text()="不限")]')
        # ganji
        if not district:
            district = response.xpath('(//a[text()="不限"])[1]/ancestor::ul[@class="f-clear"]//a[not(text()="不限")]')
        # 58
        if not district:
            district = response.xpath('(//a[text()="不限"])[1]/ancestor::div[@id="qySelectFirst"]//a[not(text()="不限")]')
        # fangdd
        if not district:
            district = response.xpath('(//a[text()="不限"])[1]/ancestor::div[@id="list_D02_10"]//a[not(text()="不限")]')
        # qfang
        if not district:
            district = response.xpath('(//a[text()="不限"])[1]/ancestor::ul[@class="search-area-detail clearfix"]//a[not(text()="不限")]')

        city_name = response.meta.get("city_name")
        station_name = response.meta.get("station_name")
        # exception handled
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
        for url in district:
            district_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            district_name = "".join(url.xpath('.//text()').extract()).strip()

            meta.update(dist_name = district_name)
            if district_name == "上海周边":
                meta.update(city_name=district_name,
                            subdist_name="其他")
                
            yield Request(url=district_url, callback=self.parse_subdistrict,
                          meta=meta)

    def parse_subdistrict(self, response):

        subdistrict = []

        # centanet
        if not subdistrict:
            self.logger.info("centanet url ...")
            subdistrict = response.xpath('(//*[text()="不限"])[2]//ancestor::p[@class="subterm fl"]'
                                              '//a[not(text()="不限")]')
        # fang
        if not subdistrict:
            self.logger.info("fang url ...")
            subdistrict = response.xpath('//p[@id="shangQuancontain"]//a[not(text()="不限")]')
        # ganji
        if not subdistrict:
            self.logger.info("ganji url ...")
            subdistrict = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]'
                                         '//a[not(text()="不限")]')
        # 58
        if not subdistrict:
            self.logger.info("58 url ...")
            subdistrict = response.xpath('//div[@id="qySelectSecond"]//a[not(text()="不限")]')
        # fangdd
        if not subdistrict:
            self.logger.info("fangdd url ...")
            subdistrict = response.xpath('(//a[text()="不限"])[2]//ancestor::'
                                         'ul[@class="_1z4Jh _2F1VP"]//a[not(text()="不限")]')
        # qfang
        if not subdistrict:
            self.logger.info("qfang url ...")
            subdistrict = response.xpath('(//a[text()="不限"])[2]//ancestor::ul'
                                         '[@class="search-area-second clearfix"]//a[not(text()="不限")]')

        dist_name = response.meta.get("dist_name")
        category = response.meta.get("category")
        station_name = response.meta.get("station_name")
        city_name = response.meta.get("city_name")
        subdist_name = response.meta.get("subdist_name")
        
        # exception handle
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

        for url in subdistrict:
            subdistrict_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            subdistrict = "".join(url.xpath('.//text()').extract()).strip()

            # 子区域替换成区域
            if city_name == "上海周边":
                dist_name = subdistrict
            else:
                subdist_name = subdistrict
                
            l = ItemLoader(item=IndexItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("url", subdistrict_url)

            l.add_value("dist_name", dist_name)
            l.add_value("subdist_name", subdist_name)
            l.add_value("category", category)
            l.add_value("station_name", station_name)
            l.add_value("city_name", city_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()


class SecondHouseIndexPageSpider(scrapy.spiders.CrawlSpider):
    name = "SecondHouseIndexPageSpider"
    category = "二手房"

    nextpage_xpaths = ['//div[@class="pagerbox"]' # centanet
                       ]
    items_xpaths = ['//a[@class="cBlueB"]' # centanet
                    ,
                    ]
    rules =(
        Rule(LinkExtractor(restrict_xpaths=nextpage_xpaths)),
        Rule(LinkExtractor(restrict_xpaths=items_xpaths),callback="parse_indexpage")
    )

    def start_requests(self):
        with sqlite3.connect(get_project_settings().get("STORE_DATABASE")) as cnx:
            cursor = cnx.cursor()
            cursor.execute("", [self.category])
            url_infos = cursor.fetchall()

        for url_info in url_infos:
            meta = {"dist_name":url_info[0],"subdist_name":url_info[1]}
            yield Request(url=url_info[2], meta=meta)

    def parse_indexpage(self, response):
        self.logger.info("process url: <%s>", response.url)
        items = []

        if ".centanet.com" in response.url:
            items = self.parse_centanet(response)

        # exception handled
        if not items:
            self.logger.error("!!!! url: %s not found any items, checkout again this  !!!!", response.url)
        for item in items:
            yield item

    def parse_centanet(self,response):
        self.logger.info("process centanet url")
        divs = response.xpath('//div[@class="house-item clearfix"]')
        for div in divs:
            l = ItemLoader(item=PropertyItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("title", '//dl[@class="fl roominfor"]//h5/text()')
            l.add_xpath("dist_name", '//div[@class="fl breadcrumbs-area f000 "]//a[@class="f000"])[3]/text()', MapCompose(lambda x: x.strip()))
            l.add_xpath("subdist_name", '//div[@class="fl breadcrumbs-area f000 "]//a[@class="f000"])[4]/text()', MapCompose(lambda x: x.strip()))
            l.add_xpath("agent_name", '//a[@class="f000 f18"]/b/text()')
            l.add_xpath("recent_activation", '//p[@class="f333"]/span[@class="f666"][1]/text()',
                        MapCompose(lambda x: int(x)), re=r"\d+")
            l.add_value("category", self.category)

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()
