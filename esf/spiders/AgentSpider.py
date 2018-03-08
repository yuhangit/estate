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
class AgentDistrictSpider(BasicDistrictSpider):
    name = "AgentDistrictSpider"

    category_name = "经纪人"
    dist_xpaths = {
        ".centanet.com": '(//*[text()="不限"])[1]//ancestor::p[@class="termcon fl"]//a[not(text()="不限")]',
        ".fang.com": '(//*[text()="不限"])[1]//ancestor::div[@class="qxName"]//a[not(text()="不限")]',
        ".ganji.com": '(//*[text()="不限"])[1]//ancestor::ul//a[not(text()="不限")]',
        ".qfang.com": '//ul[@class="search-area-detail clearfix"]//a[not(text()="不限")]',
        ".anjuke": '(//*[text()="全部"])[1]//ancestor::span[@class="elems-l"]//a[not(.//text()="全部")]',
        ".lianjia.com": '(//*[text()="不限"])[1]//ancestor::div[@class="option-list"]//a[not(text()="不限")]',
        ".5i5j.com": '(//*[text()="全部"])[1]//ancestor::ul[1]//a[not(.//text()="全部")]',
    }
    subdist_xpaths = {
        ".centanet.com": '//p[@id="PanelBlock"]/span/a',
        ".fang.com": '//p[@id="shangQuancontain"]//a[not(text()="不限")]',
        ".ganji.com": '(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]//a[not(text()="不限")]',
        ".qfang.com": '(//*[text()="不限"])[2]//ancestor::ul//a[not(text()="不限")]',
        ".anjuke.com": '(//*[text()="全部"])[2]//ancestor::div[@class="sub-items"]//a[not(.//text()="全部")]',
        ".lianjia.com": '(//*[text()="不限"])[2]//ancestor::div[@class="option-list sub-option-list"]//a[not(text()="不限")]',
        ".5i5j.com": '//dd[@class="block"]//a',
    }


class AgentPropertySpider(BasicPropertySpider):
    name = "AgentPropertySpider"
    category = "经纪人"
    domains = "房天下"
    nextpage_xpaths = {
        ".centanet.com": '//a[text()=">"]/@href',
        ".fang.com": '//a[text()="下一页"]/@href',
        ".ganji.com": '//a[@class="next"]/@href',
        ".qfang.com": '//a[@class="turnpage_next"]/@href',
        ".anjuke.com": '//a[text()="下一页 >"]/@href',
        ".lianjia.com": '//a[text()="下一页"]/@href',
        ".5i5j.com": '//a[text()="下一页"]/@href',
    }
    items_xpaths = {
        ".centanet.com": '//p[@class="name f16"]/a/@href',
        ".fang.com": '//div[@class="agent_list"]//li/@link',
        ".ganji.com": '//div[@class="broker-cont fl-l"]/a/@href',
        ".qfang.com": '//p[@class="name fl"]//a/@href',
        ".anjuke.com": '//div[@class="jjr-title"]//a/@href',
        ".lianjia.com": '//div[@class="agent-name"]/a/@href',
        ".5i5j.com": '//div[@class="agent-tit"]//a[@target="_blank"]/@href',
    }

    domains_and_parsers = {
        ".centanet.com": "parse_centanet",
        ".fang.com": "parse_fang",
        ".ganji.com": "parse_ganji",
        ".qfang.com": "parse_qfang",
        ".anjuke.com": "parse_anjuke",
        ".lianjia.com": "parse_lianjia",
        ".5i5j.com": "parse_5i5j",
    }

    def parse_centanet(self, response):
        self.logger.info("process centanet url")
        ul = response.xpath('//ul[@class="broker_list broker_listSZ"]/li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/p[@class="phone"]/b/@zvalue',
                        re=r"cnName:'(\w+)'")
            l.add_xpath("dist_name", '(//span[@class="curr"])[1]/text()')
            l.add_xpath("subdist_name", '(//span[@class="curr"])[2]/text()')
            l.add_xpath("company",'.//h2//@title')
            l.add_xpath("address", './/p[@class="xi"]//@title',
                        Join('-'))
            l.add_xpath("telephone",'.//p[@class="phone"]/b/@zvalue'
                        ,re = r"mobile:'(\w+)'")
            l.add_xpath("second_house_amount",'.//div[@class="outstanding"]//p[1]/a/text()'
                        ,re = r"\d+")
            l.add_xpath("rent_house_amount", './/div[@class="outstanding"]//p[2]/a/text()'
                        , re=r"\d+")

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

            yield l.load_item()

    def parse_fang(self, response):
        self.logger.info("process fang url")
        ul = response.xpath('//li[@link]')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/div[@class="ttop"]//a//text()')
            l.add_xpath("telephone", './/div[@class="fl"]/p[1]/text()',
                        MapCompose(lambda x: int(x)), re=r"\d+")
            l.add_xpath("company", '//li[@link]//p[@class="f14 liaxni"]/span[2]/text()',Join(','),
                        re=r"\w+")
            l.add_xpath("dist_name",'(//a[@class="orange"])[1]//text()')
            l.add_xpath("subdist_name",'(//a[@class="orange"])[2]//text()')
            l.add_xpath("second_house_amount", './/b[@class="ml03"]', re=r"(\d+)套")

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

            yield l.load_item()

    def parse_ganji(self, response):
        self.logger.info("process ganji url")
        divs = response.xpath('//div[@class="f-list-item"]')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/a[@class="broker-name"]/text()')
            l.add_xpath("address", './/span[@class="bi-text broker-xiaoqu"]//text()'
                        ,MapCompose(lambda x:x.strip()),Join())
            l.add_xpath("telephone", './/p[@class="tel"]/text()', MapCompose(lambda x: int(x)))
            l.add_xpath("dist_name", '//ul[@class="f-clear"]/li[@class="item current"]//text()')
            l.add_xpath("subdist_name",'//a[@class="subway-item current"]//text()')
            l.add_xpath("company", '//span[@class="bi-text broker-company"]/text()')

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

            yield l.load_item()

    def parse_qfang(self, response):
        self.logger.info("process qfang url")
        ul = response.xpath('//div[@id="find_broker_lists"]//li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name",'//p[@class="name fl"]//a/text()')
            l.add_xpath("dist_name", './/span[@class="con fl"]/b[1]/text()')
            l.add_xpath("subdist_name",'.//span[@class="con fl"]/b[2]/text()')
            l.add_xpath("telephone", './/div[@class="broker-tel fr"]/p/text()',
                        MapCompose(lambda x: int(x)), re = r"\d+")
            l.add_xpath("history_amount", './/span[@class="con fl"]/em/text()')

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

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
            l.add_xpath("dist_name",'(//span[@class="elems-l"]//a[@class="selected-item"])[1]//text()')
            l.add_xpath("subdist_name",'(//span[@class="elems-l"]//a[@class="selected-item"])[2]//text()')

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

            yield l.load_item()

    def parse_lianjia(self, response):
        self.logger.info("process lianjia url")
        ul = response.xpath('//ul[@class="agent-lst"]/li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/div[@class="agent-name"]//h2/text()')
            l.add_xpath("dist_name",'.//div[@class="main-plate"]//a[1]/text()', MapCompose(lambda x: x.strip()))
            l.add_xpath("subdist_name",'.//div[@class="main-plate"]//a[2]/text()', MapCompose(lambda x: x.strip()))
            l.add_xpath("telephone",'.//p[@class="mobile_p"]/text()')
            l.add_xpath("history_amount",'.//span[@class="LOGCLICKEVTID"]/text()',
                        MapCompose(lambda x: int(x)), re = r"\d+")
            l.add_xpath("recent_activation", './/div[@class="achievement"]/span/text()',
                        MapCompose(lambda x: int(x)), re = r"(\d+)套")

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

            yield l.load_item()

    def parse_5i5j(self,response):
        self.logger.info("process 5i5j  url")
        divs = response.xpath('//div[@class="list-con-box"]/div')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name",'.//h3/text()')
            l.add_xpath("dist_name", '//li[@class="new_di_tab_cur"]//text()',
                        MapCompose(lambda x:x.strip()), Join())
            l.add_xpath("subdist_name", '//dd//a[@class="cur"]//text()',
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

            # ids
            self._load_ids(l, response)
            # housekeeping
            self._load_keephouse(l, response)

            yield l.load_item()