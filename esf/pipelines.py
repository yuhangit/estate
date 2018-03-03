# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3
from scrapy.utils.project import get_project_settings as settings
import logging
from esf.items import IndexItem,PropertyItem,DistrictItem, AgentItem
from twisted.internet import defer
from twisted.enterprise import adbapi
from scrapy.exceptions import NotConfigured
import pymysql
import dj_database_url
import traceback
from scrapy import signals


class SqlitePipeline(object):
    collection_name = "scrapy_items"

    def process_item(self, item, spider):
        self.logger.info("start pipelien %s process spider %s" %(self.collection_name, spider.name))
        if isinstance(item, IndexItem):
            stmt = '''
                insert into index_pages(url, retrived, category,source, project, server, dt, spider) VALUES 
                (?,?,?,?,?,?,?,?)
            '''
            self.cursor.execute(stmt,(item.get("url"),item.get("retrived"),item.get("category"),item.get("source"),
                                      item.get("project"),item.get("server"),item.get("date"),item.get("spider")))

        elif isinstance(item, PropertyItem):

            stmt = '''insert into properties(title, url, price, address, dist_name,
                        subdist_name, dt, source, project, server,spider,agent_name,
                        agent_company,agent_phone,station_name,category, recent_activation) 
                      values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            self.cursor.execute(stmt,(item.get("title"),item.get("url"),item.get("price"),
                                      item.get("address"),item.get("dist_name"),item.get("subdist_name")
                                      ,item.get("date"),item.get("source"), item.get("project")
                                      ,item.get("server"),item.get("spider"),
                                      item.get("agent_name"),item.get("agent_company"),item.get("agent_phone")
                                      ,item.get("station_name"),item.get("category"),item.get("recent_activation")))

        elif isinstance(item, DistrictItem):
            stmt = """insert into district_rel (dist_name, subdist_name, url,category,source, project, server, dt, spider)
                        VALUES (?,?,?,?,?,?,?,?,?)
            """
            self.cursor.execute(stmt, (item.get("dist_name"), item.get("subdist_name"), item.get("url"),item.get("category")
                                       ,item.get("source"),item.get("project"), item.get("server"), item.get("date")
                                       ,item.get("spider")))

        elif isinstance(item, AgentItem):
            stmt = """insert into agencies(name,company,address, dist_name,subdist_name, telephone, history_amount,recent_activation
                          ,new_house_amount,second_house_amount,rent_house_amount,register_date,source, project, server, dt, spider)
                      values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
            self.cursor.execute(stmt,(item.get("name"),item.get("company"),item.get("address") ,item.get("dist_name")
                                      ,item.get("subdist_name"),item.get("telephone"),
                                      item.get("history_amount"),item.get("recent_activation"),
                                      item.get("new_house_amount"), item.get("second_house_amount"),item.get("rent_house_amount"),
                                      item.get("register_date"),
                                      item.get("source"),item.get("project"), item.get("server"), item.get("date"),item.get("spider")))

        self.cnx.commit()
        return item

    def open_spider(self, spider):
        self.cnx = sqlite3.connect(settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.cursor.execute("PRAGMA JOURNAL_MODE=WAL")
        self.logger = logging.Logger(__file__)

    def close_spider(self, spider):
        self.cursor.close()
        self.cnx.close()


class MysqlWriter(object):
    @classmethod
    def from_crawler(cls, crawler):
        """Retrieves scrapy crawler and accesses pipelines settings"""
        mysql_url = crawler.settings.get("MYSQL_PIPELINE_URL")
        if not mysql_url:
            raise NotConfigured

        return cls(mysql_url)

    def __init__(self, mysql_url):
        """open a mysql connection pool"""
        self.mysql_url = mysql_url
        self.report_connection_error = None

        conn_kwargs = MysqlWriter.parse_mysql_url(mysql_url)
        self.dbpool = adbapi.ConnectionPool("pymysql", charset='utf8',
                                            use_unicode=True, connect_timeout=5,
                                            **conn_kwargs)
        self.cnx = pymysql.connect(**conn_kwargs,charset="utf8")

    def open_spider(self, spider):
        self.logger = spider.logger

    def close_spider(self, spider):
        self.dbpool.close()
        self.cnx.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):

        try:
            # used for update dedicated field in district_rel

            ids = self.retrieve_id(item)
            yield self.dbpool.runInteraction(self.do_insert, item, ids)
        except pymysql.OperationalError:
            if self.report_connection_error:
                self.logger.error("Can't connect to mysql:%s",self.mysql_url)
                self.report_connection_error = False
        except:
            self.logger.exception(traceback.format_exc()+ "\n%s", item.get("url") or item.get("source") or "no url")

        defer.returnValue(item)

    def retrieve_id(self, item):
        category_id, station_id, district_id = None, None, None
        self.logger.info("item contain: %s,%s,%s,%s,%s", item.get("category"), item.get("station_name"),
                    item.get("city_name"), item.get("dist_name"), item.get("subdist_name"))
        try:
            with self.cnx.cursor() as cursor:
                category_sql = "select category_id from estate.category_rel where category_name = %s"
                cursor.execute(category_sql, (item.get("category")))
                category_id = cursor.fetchone()[0]

                station_sql = "select station_id from estate.station_rel where station_name = %s"
                cursor.execute(station_sql, (item.get("station_name")))
                station_id = cursor.fetchone()[0]

                district_sql = "select district_id from estate.district_rel where city_name = %s and " \
                               "dist_name = %s and subdist_name = %s"

                cursor.execute(district_sql, (item.get("city_name"), item.get("dist_name"), item.get("subdist_name")))
                district_id = cursor.fetchone()[0]
        except Exception as e:
            self.logger.exception("error when retrieve id")

        return {"category_id": category_id, "district_id": district_id, "station_id": station_id}

    @staticmethod
    def do_insert(tx, item, ids):

        if isinstance(item, PropertyItem):

            stmt = '''insert into properties_temp(title, url, price, address, source, project, server, dt,
                          spider, agent_name, agent_company, agent_phone, recent_activation, 
                          district_id, station_id, category_id) 
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            tx.execute(stmt, (item.get("title"), item.get("url"), item.get("price"), item.get("address"),
                              item.get("source"), item.get("project"), item.get("server"), item.get("dt"),
                              item.get("spider"), item.get("agent_name"), item.get("agent_company"),
                              item.get("agent_phone"), item.get("recent_activation"), ids.get("district_id")
                              , ids.get("station_id"), ids.get("category_id")))

        elif isinstance(item, DistrictItem):
            url_field = None
            category = item.get("category")
            if category == "新房":
                url_field = "newhouse_url"
            elif category == "二手房":
                url_field = "secondhouse_url"
            elif category == "商铺":
                url_field = "shop_url"

            if url_field:
                stmt = """update district_rel set {} = %s
                          where city_name = %s and dist_name = %s and  subdist_name = %s
                """.format(url_field)
                updated = tx.execute(stmt, (
                    item.get("url"), item.get("city_name"), item.get("dist_name"), item.get("subdist_name")
                ))
                if not updated:
                    stmt = """insert into district_rel(city_name,dist_name,subdist_name,{})
                                VALUES (%s,%s,%s,%s)""".format(url_field)
                    tx.execute(stmt, (
                       item.get("city_name"), item.get("dist_name"), item.get("subdist_name"), item.get("url")
                    ))

        elif isinstance(item, AgentItem):
            stmt = """insert into agencies_temp(name, telephone, history_amount, recent_activation, source, project,
                          spider, server, dt, second_house_amount, new_house_amount, rent_house_amount, company, 
                          address, register_date, district_id, station_id, category_id)
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            tx.execute(stmt, (
                item.get("name"), item.get("telephone"), item.get("history_amount"), item.get("recent_activation")
                , item.get("source"), item.get("project"), item.get("spider"), item.get("server"), item.get("dt"),
                item.get("second_house_amount"), item.get("new_house_amount"), item.get("recent_house_amount"),
                item.get("company"), item.get("address"), item.get("register_date"), ids.get("district_id"),
                ids.get("station_id"), ids.get("category_id")
            ))

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
        conn_kwargs = dict((k,v) for k,v in conn_kwargs.items() if v)
        return conn_kwargs