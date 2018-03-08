# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem,IndexItem,PropertyItem
from scrapy.loader import ItemLoader
from scrapehelper import DBConnect, BasicDistrictSpider,BasicPropertySpider
from urllib.parse import urlparse, urlencode
import socket
import datetime
import sqlite3
import re


# cannot use CrawlSpider for lack pass meta between rules
class ShopDistrictSpider(BasicDistrictSpider):
    name = "ShopDistrictSpider"
    category = "商铺"
    dist_xpaths = {
        # ".fang.com": '//div[@id="list_38"]//a[not(text()="不限")]',
        # ".ganji.com": '//div[@class="thr-list"]//a[not(text()="不限")]',
        # ".58.com": '//dl[@class="secitem"][1]/dd[1]/a[not(contains(text(),"全上海")) and @para="local"]',
        ".anjuke.com": '(//div[@class="elems-l"]/a[@class="selected-item"])[1]/ancestor::div[@class="elems-l"]/a[not(@rel="nofollow") and not(@class="selected-item")]',
    }
    subdist_xpaths = {
        # ".fang.com": '//p[@id="shangQuancontain"]/a[not(text()="不限")]',
        # ".ganji.com": '//div[@class="fou-list f-clear"]/a',
        # ".58.com": '//div[@id="qySelectSecond"]//a',
        ".anjuke.com": '//div[@class="sub-items"]/a[not(@class="selected-item")]',
    }

class ShopPropertySpider(BasicPropertySpider):
    name = "ShopPropertySpider"
    category = "商铺"
    domains = "安居客"
    nextpage_xpaths = {
        ".fang.com": '//div[@class="fanye gray6"]/a/@href',
        ".ganji.com": '//ul[@class="pageLink clearfix"]//a/@href',
        ".58.com": '//div[@class="pager"]//a/@href',
        ".anjuke.com": '//div[@class="multi-page"]/a/@href',
    }

    items_xpaths = {
        ".fang.com": '//p[@class="title"]/a/@href',
        ".ganji.com": '//dd[@class="dd-item title"]/a/@href',
        ".58.com": '//h2[@class="title"]/a/@href',
        ".anjuke.com": '//div[@class="list-item"]/@link',
    }

    domains_and_parsers = {
        ".fang.com": "parse_fang",
        ".ganji.com": "parse_ganji",
        ".58.com": "parse_58",
        ".anjuke.com": "parse_anjuke",
    }

    def parse_fang(self, response):
        self.logger.info("process fang url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//div[@class="title"]/h1/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="red20b"]/text()')
        l.add_xpath("address", '(//div[@class="wrap"]//dl/dt)[3]/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '//span[@id="agentname"]/text()')
        l.add_xpath("agent_company", '//dd[@class="black"]/a/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_phone", '//div[@class="phone_top"]//label[@id="mobilecode"]/text()')
        # l.add_value("category_id_shop", self.category_id_shop)
        l.add_value("station_name", "房天下")

        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

    def parse_ganji(self, response):
        self.logger.info("process ganji url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//p[@class="card-title"]/i/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="price"]/text()')
        l.add_xpath("address", '//li[@class="er-item f-fl"]/span[@class="content"]/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '//p[@class="name"]/text()', MapCompose(lambda x: x.strip()))
        l.add_xpath("agent_company", '//span[@clas="company"]/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_phone", '//div[@class="phone"]/a/text()')
        # l.add_value("category_id_secondhouse", self.category_id_secondhouse)
        l.add_value("station_name", "赶集网")

        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()

    def parse_58(self, response):
        self.logger.info("process 58 url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//div[@class="house-title"]/h1/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="house_basic_title_money_num"]/text()')
        l.add_xpath("address",
                    '//span[@class="house_basic_title_content_item3 xxdz-des"]/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '//span[@class="f14 c_333 jjrsay"]/text()', MapCompose(lambda x: x.strip()))
        # l.add_xpath("agent_company", '//span[@class="f14 c_333 jjrsay"]/text()')
        l.add_xpath("agent_phone", '//p[@class="phone-num"]/text()')
        # l.add_value("category_id_secondhouse", self.category_id_secondhouse)
        l.add_value("station_name", "58同城")

        # housekeeping
        l.add_value("source", response.request.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("dt", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        yield l.load_item()

    def parse_anjuke(self, response):
        self.logger.info("process anjuke url")

        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("title", '//div[@class="wrapper"]/h1/text()', Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_value("url", response.url)
        l.add_xpath("price", '//span[@class="price-tag"]/em/text()')
        l.add_xpath("address", '//span[@class="desc addresscommu"]/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        l.add_xpath("agent_name", '//div[@class="bro-info clearfix"]/h5/text()')
        l.add_xpath("agent_company", '//p[@class="comp_info"]/a/text()',
                    Join(), MapCompose(lambda x: "".join(x.split())))
        # l.add_value("category_id_secondhouse", self.category_id_secondhouse)
        l.add_value("station_name", "安居客")

        # ids
        self._load_ids(l, response)
        # housekeeping
        self._load_keephouse(l, response)

        yield l.load_item()
