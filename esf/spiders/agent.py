# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from esf.items import AgentItem
from scrapy.loader import ItemLoader
import socket
import datetime
import sqlite3


class AgentSpider(scrapy.Spider):
    @staticmethod
    def get_scraped_urls():
        with sqlite3.connect("data/esf_urls_test.db") as cnx:
            cursor = cnx.cursor()
            cursor.execute("SELECT DISTINCT source FROM agencies")
            return [r[0] for r in cursor.fetchall()]

    name = "AgentSpider"
    # init urls
    total_urls = []
    # lianjia
    lianjia_urls = ['https://sh.lianjia.com/jingjiren/pg%d/' %i for i in range(1, 101)]
    total_urls.extend(lianjia_urls)

    # anjuke
    anjuke_urls = ['https://shanghai.anjuke.com/tycoon/p%d/' %i for i in range(1,527)]
    total_urls.extend(anjuke_urls)

    #Q fang
    qfang_urls = ["http://shanghai.qfang.com/tycoon/o0-n%d" %i for i in range(1,31)]
    total_urls.extend(qfang_urls)

    # centanet
    centanet_urls = ["http://sh.centanet.com/jingjiren/g%d/" %i for i in range(1,303)]
    total_urls.extend(centanet_urls)

    #ganji
    ganji_urls = ["http://sh.ganji.com/fang/agent/o%d/" %i for i in range(1,51)]
    total_urls.extend(ganji_urls)

    #fang
    fang_urls = ["http://esf.sh.fang.com/agenthome/-i3%d-j3100/" %i for i in range(1,101)]
    total_urls.extend(fang_urls)

    #filter scraped index
    scraped_urls = get_scraped_urls.__func__()
    for url in scraped_urls:
        if url in total_urls:
            total_urls.remove(url)

    start_urls = total_urls

    allowed_domains = ["lianjia.com", "anjuke.com", "qfang.com", "centanet.com", "ganji.com", "fang.com"]

    # used for CrawlSpider
    rules = (
        Rule(LinkExtractor(restrict_xpaths='//a//text()[contains(.,"下一页")]//ancestor::a'),
             callback='parse_item',follow=True),
    )
    # rules = (
    #     Rule(LinkExtractor(restrict_xpaths=['//a[text()="下一页"]']),
    #          callback='parse_item', follow=True),
    # )

    def parse_item(self, response):
        self.logger.info("response url: %s", response.url)
        if response.url.find(".lianjia.com") >= 0:
           for item in self.parse_lianjia(response):
               yield item
        elif response.url.find(".anjuke.com") >= 0:
            for item in self.parse_anjuke(response):
                yield item
        elif response.url.find(".qfang.com") >= 0:
            for item in self.parse_qfang(response):
                yield item
        elif response.url.find(".centanet.com") >=0:
            for item in self.parse_centanet(response):
                yield item
        elif response.url.find(".ganji.com") >= 0:
            for item in self.parse_ganji(response):
                yield item
        elif response.url.find(".fang.com") >= 0:
            for item in self.parse_fang(response):
                yield item

    def parse(self, response):
        self.logger.info("parse start url ...")
        for item in self.parse_item(response):
            yield item

    def parse_lianjia(self, response):
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

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_anjuke(self,response):
        divs = response.xpath('//div[@class="jjr-itemmod"]')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name",".//h3/a/text()", Join())
            l.add_xpath("dist_name", './/p[@class="jjr-desc"]/a[1]/text()')
            l.add_xpath("subdist_name", './/p[@class="jjr-desc"]/a[2]/text()')
            l.add_xpath("telephone", './/div[@class="jjr-side"]/text()'
                        ,MapCompose(lambda x: int(x)),re = r"\d+")

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_qfang(self, response):
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

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_centanet(self, response):
        ul = response.xpath('//ul[@class="broker_list broker_listSZ"]/li')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/p[@class="phone"]/b/@zvalue',
                        re=r"cnName:'(\w+)'")
            l.add_xpath("dist_name",'.//h2//@title')
            l.add_xpath("subdist_name", './/p[@class="xi"]//@title',
                        Join('-'))
            l.add_xpath("telephone",'.//p[@class="phone"]/b/@zvalue'
                        ,re = r"mobile:'(\w+)'")
            l.add_xpath("second_house_amount",'.//div[@class="outstanding"]//p[1]/a/text()'
                        ,re = r"\d+")
            l.add_xpath("rent_house_amount", './/div[@class="outstanding"]//p[2]/a/text()'
                        , re=r"\d+")
            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_ganji(self, response):
        divs = response.xpath('//div[@class="f-list-item"]')
        for div in divs:
            l = ItemLoader(item=AgentItem(), selector=div)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/a[@class="broker-name"]/text()')
            l.add_xpath("dist_name", './/span[@class="bi-text"]/a[1]/text()')
            l.add_xpath("subdist_name", './/span[@class="bi-text"]/a[2]/text()')
            l.add_xpath("telephone", './/p[@class="tel"]/text()', MapCompose(lambda x: int(x)))

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_fang(self, response):
        ul = response.xpath('//li[@link]')
        for li in ul:
            l = ItemLoader(item=AgentItem(), selector=li)
            l.default_output_processor = TakeFirst()
            l.add_xpath("name", './/div[@class="ttop"]//a//text()')
            l.add_xpath("dist_name", './/span[@class="diqu"]/span[1]/text()')
            l.add_xpath("subdist_name", './/span[@class="diqu"]/span[2]/text()')
            l.add_xpath("telephone", './/div[@class="fl"]/p[1]/text()',
                        MapCompose(lambda x: int(x)), re=r"\d+")
            l.add_xpath("company", '//li[@link]//p[@class="f14 liaxni"]/span[2]/text()',Join(','),
                        re=r"\w+")

            # housekeeping
            l.add_value("source", response.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

