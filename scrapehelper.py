import dj_database_url
from scrapy.utils.project import get_project_settings
from scrapy.spiders import Rule
from scrapy import signals
from scrapy.linkextractors import LinkExtractor
from scrapy.exceptions import NotConfigured
from scrapy.loader.processors import TakeFirst, Join, MapCompose
from scrapy.loader import ItemLoader
from esf.items import IndexItem, PropertyItem, AgentItem
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from urllib.parse import urlparse, urljoin
import scrapy
import pymysql
import socket
import datetime
import abc
import traceback


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


def get_meta_info(meta):
    info_field = ["city_name", "dist_name", "subdist_name",
                  "category", "station_name", "district_id",
                  "category_id", "station_id"]
    return { k: v for k, v in meta.items() if k in info_field}


class BasicDistrictSpider(scrapy.Spider):

    category = None
    dist_xpaths = {}
    subdist_xpaths = {}
    name = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        attrs = (cls.category, cls.name, cls.dist_xpaths, cls.subdist_xpaths)
        for attr in attrs:
            if not attr:
                raise NotConfigured("attribute not configure")
        spider = cls(*args, **kwargs)
        spider.settings = crawler.settings
        spider.crawler = crawler
        crawler.signals.connect(spider.close, signals.spider_closed)
        return spider


    def start_requests(self):
        start_urls = get_project_settings().get("CATEGORIES").get(self.category)
        for url,meta in start_urls.items():
            yield Request(url=url, meta=meta, callback=self.parse_dist)

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

        meta = get_meta_info(response.meta)
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

        for xpath in self.subdist_xpaths:
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
            subdistrict = "".join(url.xpath('.//text()').extract()).strip().replace("区", "")

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


class BasicPropertySpider(scrapy.Spider):
    __metaclass__ = abc.ABCMeta # abstract class method

    category = None
    domains = None
    nextpage_xpaths = {}  # {".domain.com":"/path/to/a/@href",}
    items_xpaths = {}  # {".domain.com":"/path/to/a/@href",}

    # 因每个主站的页面差距太大, 该方法不太通用
    # @property
    # def domains_and_parsers(self):
    #     """由主站名称(.example.com)和解析字段({'type':["field_name","xpath"]})构成的列表
    #     , 用于解析具体页面, 格式如下:
    #     { ".example.com":{"xpath":[("field_name":"xpath"),(),...},
    #                      "value":[("field_name","value"),(),...],
    #                      "css":[("field_name","css"),(),...]   }
    #     """
    #     raise NotImplementedError

    @property
    def domains_and_parsers(self):
        """主站名和解析方法构成字典, 格式如下:
            domain name 格式: .domain.root
            {".example.com":"parse_example",
            ".baidu.com":"parse_baidu",
            ...
            }
        """
        raise NotImplementedError

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        attrs = ( cls.category , cls.nextpage_xpaths , cls.items_xpaths , cls.domains_and_parsers)
        for attr in attrs:
            if not attr:
                raise NotConfigured("attribute not configure")
        spider = cls(*args, **kwargs)
        spider.settings = crawler.settings
        spider.crawler = crawler
        crawler.signals.connect(spider.close, signals.spider_closed)
        return spider

    def start_requests(self):
        cnx = DBConnect.get_connect()
        # get domains name
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
            cursor.execute(stmt, (self.category,))
            items = cursor.fetchall()
        cnx.close()
        for item in items:
            url = item.pop("url")
            meta = item
            yield Request(url=url, meta=meta)

    def parse(self, response):
        meta = get_meta_info(response.meta)
        for domain, xpath in self.nextpage_xpaths.items():
            if domain in response.url:
                nextpage_urls = response.xpath(xpath).extract()
                for nextpage_url in nextpage_urls:
                    url = urljoin(response.url, nextpage_url)
                    self.logger.info("go to next page <%s> from source <%s>", url, response.url)
                    yield Request(url=url, meta=meta, callback=self.parse)

        for domain, xpath in self.items_xpaths.items():
            if domain in response.url:
                item_urls = response.xpath(xpath).extract()
                for item_url in item_urls:
                    url = urljoin(response.url, item_url)
                    self.logger.info("parse property page <%s> from source <%s>", url, response.url)
                    yield Request(url=url, meta=meta, callback=self.parse_items)

    def parse_items(self, response):
        self.logger.info("process url: <%s>", response.url)
        items = []

        for domain, parser_method in self.domains_and_parsers.items():
            if domain in response.url :
                self.logger.info("parse <%s> url <%s>", domain, response.url)
                default_parser = "parse_" + domain.split(".")[1]
                attr = getattr(self, parser_method, default_parser)
                try:
                    items = attr(response)
                except AttributeError:
                    self.logger.error(traceback.format_exc())
                break

        if not items:
            self.logger.error("!!!! url: %s not found any items, checkout again this  !!!!", response.url)
        for item in items:
            yield item

    def _load_ids(self, itemloader, response):
        "将response带来的id加载到itemloader中"
        station_id = response.meta.get("station_id")
        district_id = response.meta.get("district_id")
        category_id = response.meta.get("category_id")

        itemloader.add_value("district_id", district_id)
        itemloader.add_value("station_id", station_id)
        itemloader.add_value("category_id", category_id)

    def _load_keephouse(self, itemloader, response):
        "将辅助信息加载进itemloader中"
        itemloader.add_value("source", response.request.url)
        itemloader.add_value("project", self.settings.get("BOT_NAME"))
        itemloader.add_value("spider", self.name)
        itemloader.add_value("server", socket.gethostname())
        itemloader.add_value("dt", datetime.datetime.utcnow())

    # def get_item(self, response, field_xpaths):
    #     l = ItemLoader(item=PropertyItem(), selector=response)
    #     l.default_output_processor = TakeFirst()
    #
    #     for type,field in field_xpaths.items():
    #         if type == "xpath":
    #             l.add_xpath(field[0],field[1])
    #         elif type == "value":
    #             l.add_value(field[0],field[1])
    #         elif type == "css":
    #             l.add_css(field[0],field[1])
    #
    #     # housekeeping
    #     l.add_value("source", response.request.url)
    #     l.add_value("project", self.settings.get("BOT_NAME"))
    #     l.add_value("spider", self.name)
    #     l.add_value("server", socket.gethostname())
    #     l.add_value("dt", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #
    #     yield l.load_item()


