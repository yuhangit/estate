# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import sqlite3
from scrapy.utils.project import get_project_settings as settings
import logging


class SqlitePipeline(object):
    collection_name = "scrapy_items"

    def __init__(self):
        self.cnx = sqlite3.connect(settings().get("STORE_DATABASE"))
        self.cursor = self.cnx.cursor()
        self.cursor.execute("PRAGMA JOURNAL_MODE=WAL ")
        self.logger = logging.Logger(__file__)

    def process_item(self, item, spider):
        self.logger.info("start pipelien %s process spider %s" %(self.collection_name, spider.name))
        if spider.name.find('IndexSpider') >= 0:
            stmt = '''
                insert into index_pages(url, retrived, source, project, server, dt, spider) VALUES 
                (?,?,?,?,?,?,?)
            '''
            self.cursor.execute(stmt,(item.get("url"),item.get("retrived"),item.get("source"),
                                      item.get("project"),item.get("server"),item.get("date")
                                      ,item.get("spider")))

        elif spider.name.find('ScrapeSpider') >= 0:

            stmt = '''insert into properties(title, url, price, address, district,
                        subdistrict, dt, source, project, server,spider) 
                      values (?,?,?,?,?,?,?,?,?,?,?)'''
            self.cursor.execute(stmt,(item.get("title"),item.get("url"),item.get("price"),
                                      item.get("address"),item.get("district"),item.get("subdistrict")
                                      ,item.get("date"),item.get("source"),item.get("project")
                                      ,item.get("server"),item.get("spider")))
        self.cnx.commit()
        return item
    def check_exists(self,tbl, spider,url):
        stmt = "select count(*) from %s where spider = ? and url = ? " % tbl
        self.cursor.execute(stmt,(spider, url))
        return self.cursor.fetchall()[0][0] > 0
    def __del__(self):
        self.cursor.close()
        self.cnx.close()