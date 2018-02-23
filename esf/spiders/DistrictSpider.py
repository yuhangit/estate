# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from esf.items import DistrictItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
import socket, datetime
from urllib.parse import urlencode


class DistrictSpider(scrapy.Spider):

    name = 'DistrictSpider'
    allowed_domains = ['centanet.com', 'fang.com', 'ganji.com', '58.com','ks.js.cn',
                       'fangdd.com','qfang.com','163.com','anjuke.com','lianjia.com',
                       '5i5j.com']
    # allowed_domains = ['58.com']
    start_urls = ['http://sh.centanet.com/xinfang/', 'http://sh.centanet.com/ershoufang/','http://sh.centanet.com/jingjiren/',
                  'http://newhouse.sh.fang.com/house/s/','http://esf.sh.fang.com/','http://shop.sh.fang.com/','http://esf.sh.fang.com/agenthome/',
                  'http://sh.ganji.com/fang12/','http://sh.ganji.com/fang7/', 'http://sh.ganji.com/fang5/','http://sh.ganji.com/fang/agent/',
                  'http://sh.58.com/ershoufang/pn1/', 'http://sh.58.com/shangpucs/pn1/',
                  'http://house.ks.js.cn/secondhand.asp',
                  'http://shanghai.fangdd.com/loupan/','http://shanghai.fangdd.com/esf/',
                  'http://shanghai.qfang.com/newhouse/list','http://shanghai.qfang.com/sale','http://shanghai.qfang.com/tycoon/o0',
                  'http://xf.house.163.com/sh/search/0-0-0-0-0-0-0-0-0-1-0-0-0-0-0-1-1-0-0-0-1.html',
                  'https://shanghai.anjuke.com/tycoon/',
                  'https://sh.lianjia.com/jingjiren/',
                  'https://sh.5i5j.com/jingjiren/n0/'
                  ]

    def parse(self, response):
        # process kunshan
        if response.url.find("house.ks.js.cn") > 0:
            for item in self.parse_kunshan(response):
                yield item


        district_urls = response.xpath('(//*[text()="不限"])[1]//..//a[not(text()="不限")]')
        # ganji
        if not district_urls:
            self.logger.info("ganji/fangdd_esf/qfang/netease district ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::ul//a[not(text()="不限")]')
        # 58 商铺
        if not district_urls:
            self.logger.info("58 district ...")
            district_urls = response.xpath('//*[contains(text(),"全上海")]//..//a[not(contains(text(),"全上海"))]')
        # 安居客
        if not district_urls:
            self.logger.info("anjuke district ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//..//a[not(text()="全部")]')
        # 5a5j
        if not district_urls:
            self.logger.info("5a5j district ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//ancestor::ul[1]//a[not(.//text()="全部")]')
        for url in district_urls:
            district_url = response.urljoin(url.xpath('./@href').extract_first())
            district_name = "".join(url.xpath('.//text()').extract()).strip()

            yield Request(url=district_url, callback=self.parse_subdistrict, meta={"district_name":district_name})

    def parse_subdistrict(self, response):
        subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::p//a[not(text()="不限")]')
        # newhouse.centanet.com
        if not subdistrict_urls:
            subdistrict_urls = response.xpath('//div[@class="quyu"]//a[not(text()="不限")]')

        # ganji
        if not subdistrict_urls:
            self.logger.info("ganji subdistrict...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]//a[not(text()="不限")]')

        # 58
        if not subdistrict_urls:
            self.logger.info("58 subdsitrict ...")
            subdistrict_urls = response.xpath('//div[@id="qySelectSecond"]//a')

        # fangdd
        if not subdistrict_urls:
            self.logger.info("fangdd subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::ul//a[not(text()="不限")]')

        # 5a5j
        if not subdistrict_urls:
            self.logger.info("5a5j subdistrict ...")
            subdistrict_urls = response.xpath('//dd[@class="block"]//a')

        # anjuke
        if not subdistrict_urls:
            self.logger.info("anjuke subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="全部"])[2]//ancestor::div[@class="sub-items"]//a[not(text()="全部")]')

        # lianjia
        if not subdistrict_urls:
            self.logger.info("lianjia subdistrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="option-list sub-option-list"]//a[not(text()="不限")]')
        district = response.meta.get("district_name")
        # ！！ if no subdistrict exists, add nodef into subdistrict
        if not subdistrict_urls:
            self.logger.critical("!!!!  not subdistrict found , make sure this  !!!!")
            l = ItemLoader(item=DistrictItem())
            l.default_output_processor = TakeFirst()
            l.add_value("district", district)
            l.add_value("subdistrict", "nodef")
            l.add_value("url", response.url)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            yield l.load_item()

        for url in subdistrict_urls:
            subdistrict_url = response.urljoin(url.xpath('./@href').extract_first().strip())
            subdistrict = url.xpath('./text()').extract_first().strip()

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