import pymysql
from scrapy.exceptions import IgnoreRequest
from scrapehelper import DBConnect
from scrapy.http import Request
import logging

logger = logging.getLogger(__name__)

class SkipExistUrlMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        return cls(*args, **kwargs)

    def __init__(self, *args, **kwargs):
        self.report_connection_error = None
        self.logger = logger

    def check_exists(self, url):
        try:
            with DBConnect.get_connect().cursor() as cursor:
                cursor.execute("select count(*) from estate.properties where url = %s", (url,))
                if cursor.fetchone()[0] > 0:
                    return True
                cursor.execute("select count(*) from estate.agencies where source =%s", (url,))
                if cursor.fetchone()[0] > 0:
                    return True
            return False
        except pymysql.OperationalError as e:
            self.check_exists(url)

    def process_request(self, request, spider):
        if self.check_exists(request.url):
            raise IgnoreRequest("url <%s> has already processed" % request.url)

    def process_spider_output(self, response, result, spider):
        for x in result:
            if not isinstance(x, Request):
                yield x
            else:
                if self.check_exists(x.url):
                    self.logger.info("request<%s> have been scraped already, skip it", x.url)
                else:
                    yield x
