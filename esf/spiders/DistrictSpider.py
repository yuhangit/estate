# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from esf.items import DistrictItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
import socket, datetime
from urllib.parse import urlencode


class DistrictspiderSpider(scrapy.Spider):
    name = 'DistrictSpider'
    # allowed_domains = ['centanet.com', 'fang.com', 'ganji.com', '58.com']
    allowed_domains = ['58.com']
    # start_urls = ['http://sh.centanet.com/xinfang/', 'http://sh.centanet.com/ershoufang/','http://sh.centanet.com/jingjiren/',
    #               'http://newhouse.sh.fang.com/house/s/','http://esf.sh.fang.com/','http://shop.sh.fang.com/','http://esf.sh.fang.com/agenthome/',
    #               'http://sh.ganji.com/fang12/','http://sh.ganji.com/fang7/', 'http://sh.ganji.com/fang5/','http://sh.ganji.com/fang/agent/',
    #               'http://sh.58.com/ershoufang/pn1/', 'http://sh.58.com/shangpucs/pn1/']
    start_urls = ['http://house.ks.js.cn/secondhand.asp']

    def parse(self, response):
        # process kunshan
        if response.url.find("house.ks.js.cn")>0:
            for item in self.parse_kunshan(response):
                yield item


        district_urls = response.xpath('(//*[text()="不限"])[1]//..//a[not(text()="不限")]')
        # ganji
        if not district_urls:
            self.logger.info("ganji district ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::ul//a[not(text()="不限")]')
        # 58 商铺
        if not district_urls:
            self.logger.info("58 district ...")
            district_urls = response.xpath('//*[contains(text(),"全上海")]//..//a[not(contains(text(),"全上海"))]')

        for url in district_urls:
            district_url = response.urljoin(url.xpath('./@href').extract_first())
            district_name = url.xpath('./text()').extract_first().strip()

            yield Request(url=district_url, callback=self.parse_subdistrict, meta={"district_name":district_name})

    def parse_subdistrict(self, response):
        subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::p//a[not(text()="不限")]')
        # newhouse.centanet.com
        if not subdistrict_urls:
            subdistrict_urls  = response.xpath('//div[@class="quyu"]//a[not(text()="不限")]')

        # ganji
        if not subdistrict_urls:
            self.logger.info("ganji subdistrict...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]//a[not(text()="不限")]')

        # 58
        if not subdistrict_urls:
            self.logger.info("58 subdsitrict ...")
            subdistrict_urls = response.xpath('//div[@id="qySelectSecond"]//a')

        for url in subdistrict_urls:
            subdistrict_url = response.urljoin(url.xpath('./@href').extract_first().strip())
            subdistrict = url.xpath('./text()').extract_first().strip()

            district = response.meta.get("district_name")

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("district", district)
            l.add_value("subdistrict", subdistrict)
            l.add_value("url", subdistrict_url)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

    def parse_kunshan(self, response):
        district = "昆山"
        subdistricts = response.xpath('//option[not(text()="选择乡镇")]')
        base_url = 'http://house.ks.js.cn/secondhand.asp?'
        self.logger.info("process kunshan")
        for subdistrict in subdistricts:
            subdistrict = subdistrict.xpath("./@value").extract_first().strip()
            url = base_url + urlencode({"q":subdistrict}, encoding="gbk")

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("district", district)
            l.add_value("subdistrict", subdistrict)
            l.add_value("url", url)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

        subdistricts = response.xpath('//label[contains(@for,"q") and not(@for="q")]')
        for subdistrict in subdistricts:
            subdistrict = subdistrict.xpath("./text()").extract_first().strip()
            url = base_url + urlencode({"q": subdistrict}, encoding="gbk")

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("district", district)
            l.add_value("subdistrict", subdistrict)
            l.add_value("url", url)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()