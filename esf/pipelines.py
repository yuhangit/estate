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

    def retrieve_id(self, item):
        category_id, station_id, district_id = None, None, None
        self.logger.info("item contain: %s,%s,%s,%s,%s", item.get("category_name"), item.get("station_name"),
                    item.get("city_name"), item.get("dist_name"), item.get("subdist_name"))
        try:
            with self.cnx.cursor() as cursor:
                category_sql = "select category_id from category_rel where category_name = %s"
                cursor.execute(category_sql, (item.get("category_name"),))
                if cursor.rowcount > 0:
                    category_id = cursor.fetchone()[0]

                station_sql = "select station_id from estate.station_rel where station_name = %s"
                cursor.execute(station_sql, (item.get("station_name"),))
                if cursor.rowcount > 0:
                    station_id = cursor.fetchone()[0]

                district_sql = "select district_id from district_rel where city_name = %s and " \
                               "dist_name = %s and subdist_name = %s"

                cursor.execute(district_sql, (item.get("city_name"), item.get("dist_name"), item.get("subdist_name")))
                if cursor.rowcount > 0:
                    district_id = cursor.fetchone()[0]
        except Exception as e:
            self.logger.exception("error when retrieve id")

        return {"category_id": category_id, "district_id": district_id, "station_id": station_id}
    def process_item(self, item, spider):
        self.logger.info("start pipelien %s process spider %s" %(self.collection_name, spider.name))
        if isinstance(item, PropertyItem):

            stmt = '''insert into properties_temp(title, url, price, address, source, project, server, dt,
                          spider, agent_name, agent_company, agent_phone, recent_activation, 
                          district_id, station_id, category_id) 
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            self.cursor.execute(stmt, (item.get("title"), item.get("url"), item.get("price"), item.get("address"),
                              item.get("source"), item.get("project"), item.get("server"), item.get("dt"),
                              item.get("spider"), item.get("agent_name"), item.get("agent_company"),
                              item.get("agent_phone"), item.get("recent_activation"), ids.get("district_id")
                              , ids.get("station_id"), ids.get("category_id")))

        elif isinstance(item, DistrictItem):
            url_field = None
            category_name = item.get("category_name")
            if category_name == "新房":
                url_field = "newhouse_url"
            elif category_name == "二手房":
                url_field = "secondhouse_url"
            elif category_name == "商铺":
                url_field = "shop_url"
            elif category_name == "经纪人":
                url_field = "agency_url"

            if url_field:
                stmt = """update district_rel set {} = %s,
                              category_id = %s,
                              station_id = %s
                          where district_id = %s
                """.format(url_field)
                updated = self.cursor.execute(stmt, (
                    item.get("url"), ids.get("category_id"), ids.get("station_id"), ids.get("district_id")
                ))
                if not updated:
                    stmt = """insert into district_rel(city_name,dist_name,subdist_name,{})
                                VALUES (%s,%s,%s,%s)""".format(url_field)
                    self.cursor.execute(stmt, (
                       item.get("city_name"), item.get("dist_name"), item.get("subdist_name"), item.get("url")
                    ))

        elif isinstance(item, AgentItem):
            stmt = """insert into agencies_temp(name, telephone, history_amount, recent_activation, source, project,
                          spider, server, dt, second_house_amount, new_house_amount, rent_house_amount, company, 
                          address, register_date, district_id, station_id, category_id)
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            self.cursor.execute(stmt, (
                item.get("name"), item.get("telephone"), item.get("history_amount"), item.get("recent_activation")
                , item.get("source"), item.get("project"), item.get("spider"), item.get("server"), item.get("dt"),
                item.get("second_house_amount"), item.get("new_house_amount"), item.get("recent_house_amount"),
                item.get("company"), item.get("address"), item.get("register_date"), ids.get("district_id"),
                ids.get("station_id"), ids.get("category_id")
            ))
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
        self.settings= spider.settings

    def close_spider(self, spider):
        self.dbpool.close()
        self.cnx.close()

    @defer.inlineCallbacks
    def process_item(self, item, spider):

        try:
            # used for update dedicated field in district_rel
            ids = {}
            all_ids = self.settings.get("PROPERTY_IDS")
            for _id in all_ids:
                if item.get(_id):
                    ids.update(((_id, item.get(_id)), ))
                    all_ids.remove(_id)
                    self.logger.info("get %s from item <%s>" % (_id, item))

            self.logger.info("="*32 +"ids are %s" + "="*32, ids)
            if all_ids:
                ids.update(self.retrieve_id(item, all_ids))

            self.logger.info("="*32+ "ids are %s" + "="*32, ids)
            raise NotConfigured

            yield self.dbpool.runInteraction(self.do_insert, item, ids)
        except pymysql.OperationalError:
            if self.report_connection_error:
                self.logger.error("Can't connect to mysql:%s", self.mysql_url)
                self.report_connection_error = False
                self.__init__(self.mysql_url)
        except:
            self.logger.exception(traceback.format_exc()+ "\n%s", item.get("url") or item.get("source") or "no url")
            raise
        defer.returnValue(item)

    def retrieve_id(self, item, retrieved_ids):
        self.logger.info(retrieved_ids)
        retrieved_items = {}
        try:
            with self.cnx.cursor() as cursor:
                for retrieved_id in retrieved_ids:
                    if retrieved_id == "district_id":
                        retrieved_name = "dist_name"
                        stmt = "select district_id from estate.district_rel where city_name='{}' and" \
                               "  dist_name = %s and subdist_name = '{}'".format(item.get("city_name"),
                                                                               item.get("subdist_name"))
                    else:
                        table_name = retrieved_id.split("_")[0] + "_rel"
                        retrieved_name = retrieved_id.split("_")[0]+"_name"
                        stmt = "select {0} from {1} where {2} = %s".format(retrieved_id, table_name, retrieved_name)
                    self.logger.info("stmt: %s", stmt)
                    cursor.execute(stmt, (item.get(retrieved_name),))
                    retrieved_items[retrieved_id] = cursor.fetchone()[0] if cursor.rowcount > 0 else None

        except Exception as e:
            self.logger.exception("error when retrieve id")

        return retrieved_items


    @staticmethod
    def do_insert(tx, item, ids):

        if isinstance(item, PropertyItem):

            stmt = '''insert into estate.properties_temp(title, url, price, address, source, project, server, dt,
                          spider, agent_name, agent_company, agent_phone, recent_activation, 
                          district_id, station_id, category_id) 
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            tx.execute(stmt, (item.get("title"), item.get("url"), item.get("price"), item.get("address"),
                              item.get("source"), item.get("project"), item.get("server"), item.get("dt"),
                              item.get("spider"), item.get("agent_name"), item.get("agent_company"),
                              item.get("agent_phone"), item.get("recent_activation"), ids.get("district_id")
                              , ids.get("station_id"), ids.get("category_id")))
        # obsoleted use IndexItem  instead
        elif isinstance(item, DistrictItem):
            url_field = None
            category_field = None
            category_name = item.get("category_name")

            if category_name == "新房":
                url_field = "newhouse_url"
                category_field = "category_id_newhouse"
            elif category_name == "二手房":
                url_field = "secondhouse_url"
                category_field = "category_id_secondhouse"
            elif category_name == "商铺":
                url_field = "shop_url"
                category_field = "category_id_shop"
            elif category_name == "经纪人":
                url_field = "agency_url"
                category_field = "category_id_agency"

            if url_field:
                stmt = """update district_rel set {} = %s,
                              {} = %s,
                              station_id = %s
                          where district_id = %s
                """.format(url_field, category_field)
                updated = tx.execute(stmt, (
                    item.get("url"), ids.get("category_id"), ids.get("station_id"), ids.get("district_id")
                ))
                if not updated:
                    stmt = """insert into district_rel(city_name,dist_name,subdist_name,{})
                                VALUES (%s,%s,%s,%s)""".format(url_field)
                    tx.execute(stmt, (
                       item.get("city_name"), item.get("dist_name"), item.get("subdist_name"), item.get("url")
                    ))
        elif isinstance(item, IndexItem):

            stmt = """insert into estate.district_index_url(district_id, station_id, category_id, url) 
                    values(%s,%s,%s,%s)
            """
            tx.execute(stmt, (ids.get("district_id"), ids.get("station_id"), ids.get("category_id"), item.get("url")))
            info_stmt = """insert into estate.district_index_info(city_name, dist_name, subdist_name,
                                category_name, station_name, url, server, source, project, spider, dt) 
                         VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
            tx.execute(info_stmt, (item.get("city_name"), item.get("dist_name"), item.get("subdist_name"),
                                   item.get("category_name"), item.get("station_name"), item.get("url"),
                                   item.get("server"), item.get("source"), item.get("project"), item.get("spider"),
                                   item.get("dt")))
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