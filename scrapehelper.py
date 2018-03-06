import dj_database_url
from scrapy.utils.project import get_project_settings
from scrapy.loader.processors import TakeFirst, Join, MapCompose
from scrapy.loader import ItemLoader
from esf.items import IndexItem, PropertyItem, AgentItem
import pymysql
import socket
import datetime


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


class ScrapeHelper(object):

    def get_meta_info(self, meta):
        info_field = ["city_name", "dist_name", "subdist_name", "category", "station_name"]
        return { k: v for k, v in meta.items() if k in info_field}

    def dist_error(self, response,**kwargs):
        city_name = kwargs.get("city_name")
        dist_name = kwargs.get("dist_name")
        subdist_name = kwargs.get("subdist_name")
        category = kwargs.get("category")
        station_name = kwargs.get("station_name")

        self.logger.error("!!!! url: %s not found any districts, checkout again this  !!!!", response.url)
        l = ItemLoader(item=IndexItem())
        l.default_output_processor = TakeFirst()
        l.add_value("url", response.url)

        l.add_value("dist_name", dist_name)
        l.add_value("subdist_name", subdist_name)
        l.add_value("category", category)
        l.add_value("city_name", city_name)
        l.add_value("station_name", station_name)

        l.add_value("source", response.request.url)
        l.add_value("project", self.settings.get("BOT_NAME"))
        l.add_value("spider", self.name)
        l.add_value("server", socket.gethostname())
        l.add_value("dt", datetime.datetime.utcnow())

        yield l.load_item()