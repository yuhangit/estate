import dj_database_url
import pymysql
from scrapy.exceptions import NotConfigured, IgnoreRequest
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
        self.cursor = self.cnx.cursor()

    def process_request(self, request, spider):

        properties_retrieved_urls = [r[0] for r in
                          self.cursor.execute("select url from estate.properties").fetchall()]
        agencies_retrieved_urls = [r[0] for r in
                          self.cursor.execute("select source from estate.agencies")]
        retried_urls = properties_retrieved_urls + agencies_retrieved_urls

        if request.url in retried_urls:
            return IgnoreRequest

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

    def __del__(self):
        self.cnx.close()
