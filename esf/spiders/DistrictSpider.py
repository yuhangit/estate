# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from esf.items import DistrictItem
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst
from scrapy.utils.project import get_project_settings
import socket, datetime
from urllib.parse import urlencode, urlparse
import sqlite3


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

    def __init__(self,*args,**kwargs):
        super(DistrictSpider,self).__init__()
        # 如何setting 中定义refresh则重新刷新部分页面
        if get_project_settings().get("REFRESH_URLS"):
            self.logger.critical("refresh partial urls")
            self.start_urls = self.fresh_urls()

    def fresh_urls(self):
        with sqlite3.connect("data/esf_urls_test.db") as cnx:
            cursor = cnx.cursor()
            cursor.execute("select DISTINCT source from district_rel where subdist_name = 'nodef'")
            urls = cursor.fetchall()
            cursor.executemany("DELETE from district_rel where source = ?", urls)
            return [r[0] for r in urls]

    def parse(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        # process kunshan
        if response.url.find("house.ks.js.cn") > 0:
            for item in self.parse_kunshan(response):
                yield item

        district_urls = response.xpath('(//*[text()="不限"])[1]//..//a[not(text()="不限")]')
        # ganji
        if not district_urls:
            self.logger.info("ganji/fangdd_esf/qfang/netease dist_name ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::ul//a[not(text()="不限")]')
        # 58 商铺
        if not district_urls:
            self.logger.info("58 dist_name ...")
            district_urls = response.xpath('//*[contains(text(),"全上海")]//..//a[not(contains(text(),"全上海"))]')
        # 安居客
        if not district_urls:
            self.logger.info("anjuke dist_name ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//..//a[not(text()="全部")]')
        # 5a5j
        if not district_urls:
            self.logger.info("5a5j dist_name ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//ancestor::ul[1]//a[not(.//text()="全部")]')

        category = self.get_category(response.url)
        for url in district_urls:
            district_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            district_name = "".join(url.xpath('.//text()').extract()).strip()

            yield Request(url=district_url, callback=self.parse_subdistrict,
                          meta={"dist_name": district_name, "category": category})

    def parse_subdistrict(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::p//a[not(text()="不限")]')
        # newhouse.centanet.com
        if not subdistrict_urls:
            subdistrict_urls = response.xpath('//div[@class="quyu"]//a[not(text()="不限")]')

        # ganji
        if not subdistrict_urls:
            self.logger.info("ganji subdist_name...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]//a[not(text()="不限")]')

        # 58
        if not subdistrict_urls:
            self.logger.info("58 subdsitrict ...")
            subdistrict_urls = response.xpath('//div[@id="qySelectSecond"]//a')

        # fangdd
        if not subdistrict_urls:
            self.logger.info("fangdd subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::ul//a[not(text()="不限")]')

        # 5a5j
        if not subdistrict_urls:
            self.logger.info("5a5j subdist_name ...")
            subdistrict_urls = response.xpath('//dd[@class="block"]//a')

        # anjuke
        if not subdistrict_urls:
            self.logger.info("anjuke subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="全部"])[2]//ancestor::div[@class="sub-items"]//a[not(text()="全部")]')

        # lianjia
        if not subdistrict_urls:
            self.logger.info("lianjia subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="option-list sub-option-list"]//a[not(text()="不限")]')

        district = response.meta.get("dist_name")
        category = response.meta.get("category")

        # ！！ if no subdist_name exists, add nodef into subdist_name
        if not subdistrict_urls:
            self.logger.critical("!!!!  not subdist_name found , make sure this  !!!!")
            self.logger.critical("url: %s", response.url)
            l = ItemLoader(item=DistrictItem())
            l.default_output_processor = TakeFirst()
            l.add_value("dist_name", district)
            l.add_value("subdist_name", "nodef")
            l.add_value("url", response.url)
            l.add_value("category", category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()

        for url in subdistrict_urls:
            subdistrict_url = response.urljoin(url.xpath('./@href').extract_first().strip())
            subdistrict = url.xpath('./text()').extract_first().strip()

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("dist_name", district)
            l.add_value("subdist_name", subdistrict)
            l.add_value("url", subdistrict_url)
            l.add_value("category", category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()

    # 昆山二手房网站架构老旧, 单独处理
    def parse_kunshan(self, response):
        district = "昆山"
        subdistricts = response.xpath('//option[not(text()="选择乡镇")]')
        base_url = 'http://house.ks.js.cn/secondhand.asp?'

        category = self.get_category(response.url)
        self.logger.info("process kunshan")
        for subdistrict in subdistricts:
            subdistrict = subdistrict.xpath("./@value").extract_first().strip()
            url = base_url + urlencode({"q":subdistrict}, encoding="gbk")

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("dist_name", district)
            l.add_value("subdist_name", subdistrict)
            l.add_value("url", url)
            l.add_value("category", category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()

        subdistricts = response.xpath('//label[contains(@for,"q") and not(@for="q")]')
        for subdistrict in subdistricts:
            subdistrict = subdistrict.xpath("./text()").extract_first().strip()
            url = base_url + urlencode({"q": subdistrict}, encoding="gbk")

            l = ItemLoader(item=DistrictItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("dist_name", district)
            l.add_value("subdist_name", subdistrict)
            l.add_value("url", url)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("date", datetime.datetime.utcnow())

            yield l.load_item()

    #  获取每个主站的类别
    def get_category(self,url):
        categories = get_project_settings().get("CATEGORIES")
        category = ""
        for k, v in categories.items():
            if url in v:
                category = k
                return category
        return category