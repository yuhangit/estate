# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field

class EsfItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ScrapeItem(scrapy.Item):
    title = Field()
    url = Field()
    price = Field()
    address = Field()
    district = Field()
    subdistrict = Field()

    # housekeeping
    date = Field()
    source = Field()
    spider = Field()
    project = Field()
    server = Field()


class IndexItem(scrapy.Item):
    url = Field()
    retrived = Field()
    # housekeeping
    date = Field()
    source = Field()
    spider = Field()
    project = Field()
    server = Field()