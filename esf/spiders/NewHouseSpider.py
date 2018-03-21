# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem,IndexItem,PropertyItem
from scrapy.loader import ItemLoader
from scrapehelper import DBConnect, BasicDistrictSpider, BasicPropertySpider
from scrapehelper import get_meta_info
from urllib.parse import urlparse, urlencode
import socket
import datetime
import sqlite3
import re


# cannot use CrawlSpider for lack pass meta between rules
class NewHouseDistrictSpider(BasicDistrictSpider):
    name = "NewHouseDistrictSpider"

    category_name = "新房"
    dist_xpaths = {
         ".centanet.com":'//div[@class="termline clearfix"][1]/p[@class="termcon fl"]/a[not(text()="不限")]',
        "fang.com": '//li[@id="quyu_name"]/a[not(text()="不限")]',
        ".ganji.com": '//ul[@class="f-clear"]//a[not(text()="不限")]',
        ".fangdd.com": '//ul[@class="_3p3k4 _3MnR2"]//a[not(text()="不限")]',
        ".qfang.com": '//ul[@class="search-area-detail clearfix"]//a[not(text()="不限")]',
    }
    subdist_xpaths = {
        ".centanet.com": '//p[@id="PanelBlock"]//a[not(text()="不限")]',
        ".fang.com": '//li[@id and @_fckxhtmljob="1"]/a',
        ".ganji.com": '//div[@class="fou-list f-clear"]//a[not(text()="不限")]',
        ".fangdd.com": '//ul[@class="_3p3k4 _2yGcr"]//a[not(text()="不限")]',
        ".qfang.com": '//ul[@class="search-area-second clearfix"]//a[not(text()="不限")]',

    }

    # def start_requests(self):
    #     start_urls = get_project_settings().get("CATEGORIES")[self.category_name]
    #     for url,meta in start_urls.items():
    #         yield Request(url=url, meta=meta)
    #
    # def parse(self, response):
    #     district = []
    #
    #     info = None
    #     for xpath in self.dist_xpaths:
    #         if not district:
    #             district = response.xpath(xpath[0])
    #             info = xpath[1]
    #         else:
    #             self.logger.info("find dist_name in [%s]", info)
    #             break
    #
    #     city_name = response.meta.get("city_name")
    #     station_name = response.meta.get("station_name")
    #     category_name = response.meta.get("category_name")
    #
    #     if not district:
    #         self.logger.error("!!!! url: %s not found any districts, checkout again this  !!!!", response.url)
    #         l = ItemLoader(item=IndexItem())
    #         l.default_output_processor = TakeFirst()
    #         l.add_value("url", response.url)
    #
    #         l.add_value("dist_name", None)
    #         l.add_value("subdist_name", None)
    #         l.add_value("category_name", self.category_name)
    #         l.add_value("city_name", city_name)
    #         l.add_value("station_name", station_name)
    #
    #         l.add_value("source", response.request.url)
    #         l.add_value("project", self.settings.get("BOT_NAME"))
    #         l.add_value("spider", self.name)
    #         l.add_value("server", socket.gethostname())
    #         l.add_value("dt", datetime.datetime.utcnow())
    #
    #         yield l.load_item()
    #
    #     meta = self.get_meta_info(response.meta)

    # def parse_subdistrict(self, response):
    #     subdistrict = []
    #
    #
    #     if not subdistrict:
    #         self.logger.info("centanet url ...")
    #         subdistrict = response.xpath('//p[@id="PanelBlock"]//a[not(text()="不限")]')
    #         dist_name = response.xpath('(//p[@class="termcon fl"])[1]//a[@class="curr"]/text()').extract_first()
    #
    #     if not subdistrict:
    #         self.logger.info("fang url ...")
    #         subdistrict = response.xpath('//li[@id and @_fckxhtmljob="1"]/a')
    #         dist_name = response.xpath('(//a[@class="hui" and @href="#no"])[1]/text()').extract_first()
    #
    #     if not subdistrict:
    #         self.logger.info("ganji url ...")
    #         subdistrict = response.xpath('//div[@class="fou-list f-clear"]//a[not(text()="不限")]')
    #         dist_name = response.xpath('//div[@class="thr-list"]//li[@class="item current"]/a/text()').extract_first()
    #
    #     if not subdistrict:
    #         self.logger.info("fangdd url ...")
    #         subdistrict = response.xpath('//ul[@class="_3p3k4 _2yGcr"]//a[not(text()="不限")]')
    #         dist_name = response.xpath('//li[@class="aaK37 rwyU0 _2xI7o"]/a/text()').extract_first()
    #
    #     if not subdistrict:
    #         self.logger.info("Qfang url ...")
    #         subdistrict = response.xpath('//ul[@class="search-area-second clearfix"]//a[not(text()="不限")]')
    #         dist_name = response.xpath('//ul[@class="search-area-detail clearfix"]//a[@class="current"]/text()').extract_first()
    #
    #         if dist_name == "周边城市":
    #             city_name = "上海周边"
    #             dist_name = "不限"
    #
    #     # handle exception
    #     if not subdistrict:
    #         self.logger.critical("!!!! url: <%s> not  found any sub_districts, checkout again  !!!!", response.url)
    #         l = ItemLoader(item=IndexItem())
    #         l.default_output_processor = TakeFirst()
    #
    #         l.add_value("url", response.url)
    #
    #         l.add_value("dist_name", dist_name)
    #         l.add_value("subdist_name", None)
    #         l.add_value("category_name", category_name)
    #         l.add_value("city_name", city_name)
    #         l.add_value("station_name", station_name)
    #
    #         l.add_value("source", response.request.url)
    #         l.add_value("project", self.settings.get("BOT_NAME"))
    #         l.add_value("spider", self.name)
    #         l.add_value("server", socket.gethostname())
    #         l.add_value("dt", datetime.datetime.utcnow())
    #
    #         yield l.load_item()
import pymysql

class NewHousePropertySpider(BasicPropertySpider):
    name = "NewHousePropertySpider"
    # domains = "房天下"
    category_name = "新房"
    nextpage_xpaths = {
        ".ganji.com": '//ul[@class="pageLink clearfix"]//a/@href',
        ".fang.com": '//li[@class="fr"]/a/@href',
        ".fangdd.com": '//div[@class="ZDLI8"]/a/@href',
        ".qfang.com": '//p[@class="turnpage_num fr"]/a[not(@class="cur")]',
    }
    items_xpaths = {
        ".centanet.com": '//h5[@class="room-name"]/a/@href',
        ".fang.com": '//div[@class="nlcd_name"]/a/@href' ,
        ".ganji.com": '//div[@class="f-list-item ershoufang-list"]//dd/a/@href',
        ".fangdd.com": '//div[@class="vzBCU"]//p[@class="_1AXFZ"]/a[1]/@href',
        ".qfang.com": '//div[@class="show-detail fl"]/p[@class="house-title"]/a/@href',
    }

# temp
    def start_requests(self):
        cnx = DBConnect.get_connect()
        # get domains name
        with cnx.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("""
            select url,district_id,category_id,station_id
          from district_index_url
          where district_id in (select district_id from district_rel 
        where city_name = "上海周边" and dist_name = "昆山")
	      and station_id != 12
        and category_id = (select category_id from category_rel where category_name = "新房");
            """)
            items = cursor.fetchall()
        cnx.close()
        for item in items:
            url = item.pop("url")
            meta = item
            yield Request(url=url, meta=meta)

    def parse_centanet(self, response):
        self.logger.info("process centanet url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//h5[@class="mr25 f16 "]/a/text()',
                    MapCompose(lambda x: "".join(x.split())))
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="nhpice"]/b/text()')
        l.add_xpath("address", '(//p[@class="txt_r"])[1]/text()')
        l.add_xpath("agent_name", '//span[@class="f000 f18 mr6"]/text()')
        # l.add_value("category_id_shop", self.category_id_shop)
        # l.add_value("station_name", "中原地产")

        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

    def parse_fang(self, response):
        self.logger.info("process fang url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//div[@class="tit"]/h1/strong/text()')
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="prib cn_ff"]/text()')
        l.add_xpath("address", '//div[@class="inf_left fl"]/span/text()')
        l.add_xpath("agent_name", '//dt[@class="wai"]/a/text()')
        l.add_xpath("agent_company", '//li[@class="tf cl_333"]/a/text()')
        # l.add_value("category_id_shop", self.category_id_shop)
        # l.add_value("station_name", "房天下")
        # category_name
        # station_name
        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

    def parse_ganji(self, response):
        self.logger.info("process ganji url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//p[@class="card-title"]/i/text()')
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="price"]/text()')
        l.add_xpath("address", '(//li[@class="er-item f-fl"])[2]/span[@class="content"]/a/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '//p[@class="name"]/text()', MapCompose(lambda x: x.strip()))
        l.add_xpath("agent_company", '//span[@clas="company"]/text()')
        l.add_xpath("agent_phone", '//a[@class="phone_num js_person_phone"]/text()', Join()(), re="(\\d+)")


        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

    def parse_fangdd(self, response):
        self.logger.info("process fangdd url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//h1[@class="_3sWIj"]/text()',
                    MapCompose(lambda x: "".join(x.split())))
        l.add_value("url", response.url)
        l.add_xpath("price", '//div[@class="C1hVk"]/text()',
                    MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("address",
                    '//div[@class="_2mmF- _3YJ15 undefined"]/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '//span[@class="zMrme"]/text()', MapCompose(lambda x: x.strip()))
        # l.add_value("category_id_secondhouse", self.category_id_secondhouse)
        # l.add_value("station_name", "房多多")

        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

    def parse_qfang(self, response):
        self.logger.info("process qfang url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//h2[@class="house-title fl"]/text()', MapCompose(lambda x: x.strip()))
        l.add_value("url", response.url)
        l.add_xpath("price", '//p[@class="newhs-average-price fl"]/span/text()')
        l.add_xpath("address", '//p[@class="project-address clearfix"]/em/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '(//p[@class="name"]/span)[1]/text()')

        # l.add_value("category_id_secondhouse", self.category_id_secondhouse)
        # l.add_value("station_name", "Q房网")

        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

