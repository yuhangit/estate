import dj_database_url
from scrapy.utils.project import get_project_settings
import pymysql

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