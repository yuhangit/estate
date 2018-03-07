import pymysql
from scrapy.exceptions import IgnoreRequest
from scrapehelper import DBConnect


class SkipExistUrlMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        return cls(*args, **kwargs)

    def __init__(self, *args, **kwargs):
        self.report_connection_error = None

    def check_exists(self, url):
        try:
            with DBConnect.get_connect().cursor() as cursor:
                cursor.execute("select count(*) from estate.properties_temp where url = %s", (url,))
                if cursor.fetchone()[0] > 0:
                    return True
                cursor.execute("select count(*) from estate.agencies_temp where source =%s", (url,))
                if cursor.fetchone()[0] > 0:
                    return True
            return False
        except pymysql.OperationalError as e:
            self.check_exists(url)

    def process_request(self, request, spider):
        if self.check_exists(request.url):
            raise IgnoreRequest("url <%s> has already processed" % request.url)
