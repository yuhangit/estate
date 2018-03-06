import dj_database_url
from scrapy.utils.project import get_project_settings
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.loader.processors import TakeFirst, Join, MapCompose
from scrapy.loader import ItemLoader
from esf.items import IndexItem, PropertyItem, AgentItem
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from urllib.parse import urlparse
import scrapy
import pymysql
import socket
import datetime
import abc


class DBConnect:
    @staticmethod
    def parse_mysql_url(mysql_url):
        params = dj_database_url.parse(mysql_url)
        conn_kwargs = {}
        conn_kwargs["host"] = params["HOST"]
        conn_kwargs["user"] = params["USER"]
        conn_kwargs["passwd"] = params["PASSWORD"]
        conn_kwargs["db"] = params["NAME"]
        conn_kwargs["port"] = params["PORT"]
        # remove items with empty values
        conn_kwargs = dict((k, v) for k, v in conn_kwargs.items() if v)
        return conn_kwargs

    @staticmethod
    def get_connect():
        url = get_project_settings().get("MYSQL_PIPELINE_URL")
        paras = DBConnect.parse_mysql_url(url)
        cnx = pymysql.connect(**paras, charset='utf8',use_unicode=True,)
        return cnx


class BasicDistrictSpider(scrapy.Spider):

    @property
    def category(self):
        raise NotImplementedError("house category: newhouse, secondhouse ect")

    @property
    def dist_xpaths(self):
        raise NotImplementedError("dist_xpaths must be iterable")
    @property
    def subdist_xpaths(self):
        raise NotImplementedError("subdist_xpaths must be iterable")
    @property
    def name(self):
        raise NotImplementedError

    def start_requests(self):
        start_urls = get_project_settings().get("CATEGORIES").get(self.category)
        for url,meta in start_urls.items():
            yield Request(url=url, meta=meta, callback=self.parse_dist)

    @staticmethod
    def get_meta_info(meta):
        info_field = ["city_name", "dist_name", "subdist_name", "category", "station_name"]
        return { k: v for k, v in meta.items() if k in info_field}

    def parse_dist(self, response):
        district = []

        info = None
        for xpath in self.dist_xpaths:
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
            l.add_value("category", category)
            l.add_value("city_name", city_name)
            l.add_value("station_name", station_name)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()

        meta = self.__class__.get_meta_info(response.meta)
        for url in district:
            district_url = response.urljoin(urlparse(url.xpath('./@href').extract_first()).path)
            district_name = "".join(url.xpath('.//text()').extract()).strip().replace("区", "")

            meta.update(dist_name=district_name)

            if district_name == "上海周边":
                meta.update(city_name=district_name, subdist_name="其他")

            yield Request(url=district_url, callback=self.parse_subdistrict, meta=meta)

    def parse_subdistrict(self, response):
        """
        在添加新的response时, 要依次测试每个xpath, xpath排列规则, 专有的站上面,
        普适在下面, 否则可能拿到错误的信息.
        :param response:
        :return:
        """
        ### 得到子区域列表
        subdistrict_urls = []
        info = None

        for xpath in self.dist_xpaths:
            if not subdistrict_urls:
                subdistrict_urls = response.xpath(xpath[0])
                info = xpath[1]
            else:
                self.logger.info("find dist_name in [%s]", info)
                break

        city_name = response.meta.get("city_name")
        category = response.meta.get("category")
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
            l.add_value("category", category)
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
            l.add_value("category", category)

            l.add_value("source", response.request.url)
            l.add_value("project", self.settings.get("BOT_NAME"))
            l.add_value("spider", self.name)
            l.add_value("server", socket.gethostname())
            l.add_value("dt", datetime.datetime.utcnow())

            yield l.load_item()


class BasicPropertySpider(scrapy.spiders.CrawlSpider):
    __metaclass__ = abc.ABCMeta # abstract class method

    @property
    def category(self):
        raise NotImplementedError
    @property
    def nextpage_xpaths(self):
        raise NotImplementedError
    @property
    def items_xpaths(self):
        raise NotImplementedError

    @property
    def domains_and_parsers(self):
        """由主站名称(.example.com)和解析字段({"field_name":"xpath"})构成的列表
        , 用于解析具体页面, 格式如下:
        { ".example.com":{"field_name":"xpath",...},...}
        """
        raise NotImplementedError

    rules = (
        Rule(LinkExtractor(restrict_xpaths=nextpage_xpaths)),
        Rule(LinkExtractor(restrict_xpaths=items_xpaths), callback="parse_page")
    )

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.settings = get_project_settings()

    @abc.abstractclassmethod
    def parse_page(self, response):
        self.logger.info("process url: <%s>", response.url)
        items = []

        for domain, field_xpaths in self.domains_and_parsers.items():
            if domain in response.url :
                self.logger.info("parse <%s> url <%s>", domain, response.url)
                items = self.get_item(response, field_xpaths)
                break

        if not items:
            self.logger.error("!!!! url: %s not found any items, checkout again this  !!!!", response.url)
        for item in items:
            yield item

    def get_item(self, response, field_xpaths):
        l = ItemLoader(item=PropertyItem(), selector=response)
        l.default_output_processor = TakeFirst()

        l.add_xpath()

        # housekeeping
        l.add_value("source", response.request.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("dt", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        yield l.load_item()


