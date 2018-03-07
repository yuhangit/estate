# -*- coding: utf-8 -*-
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.loader.processors import TakeFirst, Join , MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from esf.items import AgentItem,IndexItem,PropertyItem
from scrapy.loader import ItemLoader
from scrapehelper import DBConnect, BasicDistrictSpider
from urllib.parse import urlparse, urlencode
import socket
import datetime
import sqlite3
import re


# cannot use CrawlSpider for lack pass meta between rules
class ShopDistrictSpider(BasicDistrictSpider):
    name = "ShopDistrictSpider"

    category = "商铺"
    dist_xpaths = [
        ['//div[@id="list_38"]//a[not(text()="不限")]', "房天下"],
        ['//div[@class="thr-list"]//a[not(text()="不限")]',  '赶集网'],
        ['//dl[@class="secitem"][1]//a[not(contains(text(),"全上海"))]', '58同城'],
    ]
    subdist_xpaths = [
        ['//p[@id="shangQuancontain"]//a[not(text()="不限")]', '房天下'],
        ['//div[@class="fou-list f-clear"]//a[not(text()="不限")]', '赶集网'],
        ['//dl[@class="secitem secitem-fist"]//a[not(contains(text(),"全上海"))]', '58同城'],
    ]
