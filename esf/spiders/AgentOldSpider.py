# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem, DistrictItem,IndexItem
from scrapy.loader import ItemLoader
from scrapehelper import DBConnect,BasicDistrictSpider
from scrapehelper import get_meta_info
from urllib.parse import urlparse,urlencode
import socket
import datetime
import sqlite3
import re
import pymysql


class AgentOldDistrictSpider(BasicDistrictSpider):
    name = "AgentOldDistrictSpider"
    category_name = "经纪人"
    start_urls = get_project_settings().get("CATEGORIES")[category_name]

    def start_requests(self):
        for url, meta in self.start_urls.items():
            yield Request(url, meta=meta)

    def parse(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        district_urls = []
        ###  得到区域列表
        # 5a5j
        if not district_urls:
            self.logger.info("5a5j dist_name ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//ancestor::ul[1]//a[not(.//text()="全部")]')
        # lianjia
        if not district_urls:
            self.logger.info("lianjia dist_name ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::div[@class="option-list"]//a[not(text()="不限")]')
        # ganji
        if not district_urls:
            self.logger.info("ganji dist_name ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::ul//a[not(text()="不限")]')
        # 58 商铺
        if not district_urls:
            self.logger.info("58 dist_name ...")
            district_urls = response.xpath('//*[contains(text(),"全上海")]//..//a[not(contains(text(),"全上海"))]')
        # 安居客
        if not district_urls:
            self.logger.info("anjuke dist_name ...")
            district_urls = response.xpath('(//*[text()="全部"])[1]//ancestor::span[@class="elems-l"]//a[not(.//text()="全部")]')
        # fang
        if not district_urls:
            self.logger.info("fang dist_name ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::div[@class="qxName"]//a[not(text()="不限")]')
        # centanet
        if not district_urls:
            self.logger.info("centanet dist_name ...")
            district_urls = response.xpath('(//*[text()="不限"])[1]//ancestor::p[@class="termcon fl"]//a[not(text()="不限")]')
        ###
        station_name = response.meta.get("station_name")
        city_name = response.meta.get("city_name")
        # exception handled
        if not district_urls:
            self.logger.error("!!!! url: %s not found any districts, checkout again this  !!!!", response.url)
            l = ItemLoader(item=IndexItem())
            l.default_output_processor = TakeFirst()
            l.add_value("url", response.url)

            l.add_value("dist_name", None)
            l.add_value("subdist_name", None)
            l.add_value("city_name", city_name)
            l.add_value("category_name", self.category_name)
            l.add_value("station_name", station_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()

        meta = get_meta_info(response.meta)
        for url in district_urls:
            district_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            district_name = "".join(url.xpath('.//text()').extract()).strip().replace("区", "")

            meta.update(dist_name=district_name)

            if district_name == "上海周边":
                meta.update(city_name=district_name,
                            subdist_name="其他")

            yield Request(url=district_url, callback=self.parse_subdistrict,
                          meta=meta)

    def parse_subdistrict(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        ### 得到子区域列表
        subdistrict_urls = []
        # 5i5j
        if not subdistrict_urls:
            self.logger.info("5a5j subdist_name ...")
            subdistrict_urls = response.xpath('//dd[@class="block"]//a')
        # lainjia
        if not subdistrict_urls:
            self.logger.info("lianjian subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="option-list sub-option-list"]//a[not(text()="不限")]')
        #anjuke
        if not subdistrict_urls:
            self.logger.info("anjuke subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="全部"])[2]//ancestor::div[@class="sub-items"]//a[not(.//text()="全部")]')
        # qfang
        if not subdistrict_urls:
            self.logger.info("qfang subdsitrict ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::ul//a[not(text()="不限")]')
        # ganji
        if not subdistrict_urls:
            self.logger.info("ganji subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::div[@class="fou-list f-clear"]//a[not(text()="不限")]')
        if not subdistrict_urls:
            self.logger.info("fang subdist_name ...")
            subdistrict_urls = response.xpath('//p[@id="shangQuancontain"]//a[not(text()="不限")]')
        if not subdistrict_urls:
            self.logger.info("centanet subdist_name ...")
            subdistrict_urls = response.xpath('(//*[text()="不限"])[2]//ancestor::p[@class="subterm fl"]//a[not(text()="不限")]')
        ###
        ##
        city_name = response.meta.get("city_name")
        category_name = response.meta.get("category_name")
        dist_name = response.meta.get("dist_name")
        subdist_name = response.meta.get("subdist_name")
        station_name = response.meta.get("station_name")


        ### 若子区域列表为空 插入一条subdistrict 为nodef的数据.
        if not subdistrict_urls:
            self.logger.critical("!!!! url: <%s> not  found any sub_districts, checkout again  !!!!", response.url)
            l = ItemLoader(item=IndexItem())
            l.default_output_processor = TakeFirst()

            l.add_value("url", response.url)

            l.add_value("dist_name", dist_name)
            l.add_value("subdist_name", None)
            l.add_value("category_name", category_name)
            l.add_value("city_name", city_name)
            l.add_value("station_name", station_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()

        for url in subdistrict_urls:
            subdistrict_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            subdistrict = "".join(url.xpath('.//text()').extract()).strip()
            self.logger.info("subdistrict name: <%s>", subdist_name)

            # 子区域替换成区域
            if city_name == "上海周边":
                dist_name = subdistrict
            else:
                subdist_name = subdistrict

            l = ItemLoader(item=IndexItem(), selector=url)
            l.default_output_processor = TakeFirst()
            l.add_value("dist_name", dist_name)
            l.add_value("city_name", city_name)
            l.add_value("station_name", station_name)
            l.add_value("subdist_name", subdist_name)
            l.add_value("url", subdistrict_url)
            l.add_value("category_name", category_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()



class AgencyOldSpider(scrapy.spiders.Spider):
    name = "AgencyOldSpider"
    category_name = "经纪人"
    domains = None
    xpaths = {
        ".ganji.com": '//ul[@class="pageLink clearfix"]//a[not(contains(@href,"javascript"))]/@href', # ganji
        ".centanet.com": '//div[@class="pagerbox"]//a[not(contains(@href,"javascript"))]/@href',                  # centanet
        ".fang.com": '//div[@id="agentlist_B08_01"]/a[not(contains(@href,"javascript"))]/@href',                # 5i5j, fang
              # '//div[@class="page-box house-lst-page-box"]//a', # lianjia
        ".anjuke.com": '//div[@class="multi-page"]//a[not(contains(@href,"javascript"))]/@href',                         # anjuke
        ".qfang.com": '//p[@class="turnpage_num"]//a[not(contains(@href,"javascript"))]/@href',      # qfang
        ".5i5j.com": '//div[@class="pageSty rf"]/a[not(contains(@href,"javascript"))]/@href'
    }

    def start_requests(self):
        cnx = DBConnect.get_connect()
        if not self.domains:
            stmt = """select url ,district_id,category_id,station_id
                          from estate.district_index_url 
                          where category_id in (
                            SELECT category_id 
                            from estate.category_rel
                            where category_name = %s
                          )  and district_id is not NULL 
                        """
        else:
            domain = "( %s )" % ",".join(map(lambda x: "'%s'" %x, self.domains)) if not isinstance(self.domains, str) else "('%s')" % self.domains
            stmt = """select url,district_id,category_id,station_id
                      from estate.district_index_url 
                      where category_id in (
                        SELECT category_id 
                        from estate.category_rel
                        where category_name = %s
                      ) and station_id in (
                        select station_id
                        from estate.station_rel
                        where station_name in {}
                      ) and district_id is not NULL """.format(domain)

        with cnx.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(stmt, (self.category_name,))
            items = cursor.fetchall()
        cnx.close()
        for item in items:
            url = item.pop("url")
            meta = item
            yield Request(url=url, meta=meta)

    def parse(self, response):
        meta = get_meta_info(response.meta)
        self.logger.info("*"*32+"meta: %s", meta)
        # 链家页面页码由 js生成
        if '.lianjia.com' in response.url:
            r = re.compile('{page}')
            page_num = response.xpath('//div[@class="page-box house-lst-page-box"]/@page'
                                          '-data').re_first(r":(\d+),")
            base_path = response.xpath('//div[@class="page-box house-lst-page-box"]/@page-url').extract_first()

            # 若该url没有页码标记,直接解析
            if page_num:
                for i in range(1,int(page_num)+1):
                    url_path = response.urljoin(r.sub("%i" %i, base_path))
                    yield Request(url=url_path, callback=self.parse_indexpage,meta=meta)
            else:
                for item in self.parse_indexpage(response):
                    yield item
        # 利用Rules 爬取5i5j后续页面缓慢
        # elif '.5i5j.com' in response.url:
        #     r = re.compile('\\d+$')
        #     page_num = response.xpath('//div[@class="pageSty rf"]//a//text()').re(r'(\d+)')
        #     base_path = response.xpath('//div[@class="pageSty rf"]//a[not(@class="cur")]//@href').extract_first()
        #     if page_num:
        #         for i in range(1,max(map(int,page_num))+1):
        #             url_path = response.urljoin(r.sub("%i/" %i,base_path))
        #             self.logger.critical("url: %s",url_path)
        #             yield Request(url=url_path, callback=self.parse_indexpage, meta=meta)
        #
        #     else:
        #         for item in self.parse_indexpage(response):
        #             yield item
        else:
            for item in self.parse_indexpage(response):
                yield item

            for domain, xpath in self.xpaths:
                if domain in response.url:
                    nextpage_urls = response.xpath(xpath).extract()
                    for url in nextpage_urls:
                        url = response.urljoin(url)
                        yield Request(url, meta=meta)

    def parse_indexpage(self,response):
        self.logger.info("process url: <%s>", response.url)
        items = []

        if ".lianjia.com" in response.url:#
            items = self.parse_lianjia(response)
        elif ".anjuke.com" in response.url:#
            items = self.parse_anjuke(response)
        elif ".qfang.com" in response.url:#
            items = self.parse_qfang(response)
        elif ".centanet.com" in response.url:#
            items = self.parse_centanet(response)
        elif ".ganji.com" in response.url:#
            items = self.parse_ganji(response)
        elif ".fang.com" in response.url:#
            items = self.parse_fang(response)
        elif ".5i5j.com" in response.url: #
            items = self.parse_5i5j(response)
        else:
            self.logger.critical("not parse find")

        for item in items:
            yield item

    def _loads_ids(self, l, response):
        l.add_value("district_id", response.meta.get("district_id"))
        l.add_value("category_id", response.meta.get("category_id"))
        l.add_value("station_id", response.meta.get("station_id"))

    def _loads_housekeeping(self, l, response):
        l.add_value("source", response.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("dt", datetime.datetime.utcnow())

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
            self._loads_ids(l,response)
            #  housekeeping
            self._loads_housekeeping(l, response)

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
            self._loads_ids(l, response)
            #  housekeeping
            self._loads_housekeeping(l, response)

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
            self._loads_ids(l, response)
            #  housekeeping
            self._loads_housekeeping(l, response)

            yield l.load_item()

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
            self._loads_ids(l, response)
            #  housekeeping
            self._loads_housekeeping(l, response)

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
            self._loads_ids(l, response)
            #  housekeeping
            self._loads_housekeeping(l, response)

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
            self._loads_ids(l, response)
            #  housekeeping
            self._loads_housekeeping(l, response)

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
            self._loads_ids(l, response)
            #  housekeeping
            self._loads_housekeeping(l, response)

            yield l.load_item()


class TestCrawlSpider(scrapy.spiders.CrawlSpider):
    name = "TestCrawlSpider"
    xpaths = '//a[text()="下一页"]'  # 5i5j, fang


    rules = (
        # ganji
        Rule(LinkExtractor(restrict_xpaths=xpaths),
             callback='parse_indexpage', follow=True),
    )

    def start_requests(self):
      yield Request(url='https://sh.5i5j.com/jingjiren/xinzhuang/')

    def parse_start_url(self, response):

        if '.lianjia.com' in response.url:
            r = re.compile('{page}')
            page_num = int(response.xpath('//div[@class="page-box house-lst-page-box"]/@page'
                                          '-data').re_first(r":(\d+),"))
            base_path = response.xpath('//div[@class="page-box house-lst-page-box"]/@page-url').extract_first()
            for i in range(1,page_num+1):
                url_path = response.urljoin(r.sub("%i" %i, base_path))
                yield Request(url=url_path, callback=self.parse_indexpage)
        elif '.5i5j.com' in response.url:
            r = re.compile('\\d+$')
            page_num = response.xpath('//div[@class="pageSty rf"]//a//text()').re(r'(\d+)')
            base_path = response.xpath('//div[@class="pageSty rf"]//a[not(@class="cur")]//@href').extract_first()
            if page_num:
                for i in range(1, max(map(int, page_num))+1):
                    url_path = response.urljoin(r.sub("%i/" %i, base_path))
                    self.logger.critical("url: %s", url_path)
                    yield Request(url=url_path, callback=self.parse_indexpage)

            else:
                for item in self.parse_indexpage(response):
                    yield item

        else:
            for item in self.parse_indexpage(response):
                yield item

    def parse_indexpage(self, response):
        yield {"url":response.url}


from scrapy.http import FormRequest


class TestFormSpider(scrapy.spiders.Spider):
    start_urls = ["http://sh.centanet.com/ershoufang/shcns000009401.html"]
    name = "TestFormSpider"
    api_url = 'http://sh.centanet.com/page/v1/ajax/agent400.aspx'

    def parse(self, response):
        postid = response.xpath('//li[@class="collect"]/a/@para').re_first(r"postid:'([\w-]+)'")
        type = "post"
        formdata = {"postid":postid, "type":type}

        title = response.xpath('//dl[@class="fl roominfor"]//h5/text()').extract_first()
        district = response.xpath('(//div[@class="fl breadcrumbs-area f000 "]//a[@class="f000"])[3]/text()').extract_first()
        subdistrict =response.xpath('(//div[@class="fl breadcrumbs-area f000 "]//a[@class="f000"])[4]/text()').extract_first()
        agent_name = response.xpath( '//a[@class="f000 f18"]/b/text()').extract_first()

        recent_activation = response.xpath('//p[@class="f333"]/span[@class="f666"][1]/text()').re(r"\d+")

        meta = {"title":title, "dist_name":district, "subdist_name":subdistrict,
                "agent_name":agent_name,"recent_activation":recent_activation}
        yield FormRequest(url=self.api_url, formdata=formdata,method="GET", meta=meta,callback=self.parse_item)

    def parse_item(self, response):
        print(response.text)
        r = re.compile("[\\d-]+")
        telephone = ":".join([i.replace("-", "") for i in r.findall(response.text)])
        print(telephone)
        meta = response.meta
        meta.setdefault("telephone", telephone)
        return meta
