# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3
from scrapy.utils.project import get_project_settings as settings
import logging
from esf.items import IndexItem,ScrapeItem,DistrictItem, AgentItem

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

        elif isinstance(item, ScrapeItem):

            stmt = '''insert into properties(title, url, price, address, district,
                        subdistrict, dt, source, project, server,spider,agent_name,
                        agent_company,agent_phone,source_name,category, recent_activation) 
                      values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            self.cursor.execute(stmt,(item.get("title"),item.get("url"),item.get("price"),
                                      item.get("address"),item.get("district"),item.get("subdistrict")
                                      ,item.get("date"),item.get("source"), item.get("project")
                                      ,item.get("server"),item.get("spider"),
                                      item.get("agent_name"),item.get("agent_company"),item.get("agent_phone")
                                      ,item.get("source_name"),item.get("category"),item.get("recent_activation")))

        elif isinstance(item, DistrictItem):
            stmt = """insert into district (district, subdistrict, url,category,source, project, server, dt, spider)
                        VALUES (?,?,?,?,?,?,?,?,?)
            """
            self.cursor.execute(stmt, (item.get("district"), item.get("subdistrict"), item.get("url"),item.get("category")
                                       ,item.get("source"),item.get("project"), item.get("server"), item.get("date")
                                       ,item.get("spider")))

        elif isinstance(item, AgentItem):
            stmt = """insert into agencies(name,company,address, district,subdistrict, telephone, history_amount,recent_activation
                          ,new_house_amount,second_house_amount,rent_house_amount,register_date,source, project, server, dt, spider)
                      values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
            self.cursor.execute(stmt,(item.get("name"),item.get("company"),item.get("address") ,item.get("district")
                                      ,item.get("subdistrict"),item.get("telephone"),
                                      item.get("history_amount"),item.get("recent_activation"),
                                      item.get("new_house_amount"), item.get("second_house_amount"),item.get("rent_house_amount"),
                                      item.get("register_date"),
                                      item.get("source"),item.get("project"), item.get("server"), item.get("date"),item.get("spider")))

        self.cnx.commit()
        return item

    def open_spider(self, spider):
        self.cnx = sqlite3.connect(settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.cursor.execute("PRAGMA JOURNAL_MODE=WAL ")
        self.logger = logging.Logger(__file__)

    def close_spider(self, spider):
        self.cursor.close()
        self.cnx.close()