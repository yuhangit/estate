import dj_database_url
import pymysql
from scrapy.exceptions import NotConfigured
from twisted.internet import defer
from twisted.enterprise import adbapi

class SkipExistUrlMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler):
        mysql_url = crawler.settings.get("MYSQL_PIPELINE_URL")
        if not mysql_url:
            raise NotConfigured

        return cls(mysql_url)

    def __init__(self, mysql_url):
        self.mysql_url = mysql_url
        self.report_connection_error = None

        conn_kwargs = self.__class__.parse_mysql_url(mysql_url)
        self.cnx = pymysql.connect(**conn_kwargs)

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
