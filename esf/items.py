# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class EsfItem(scrapy.Item):
    pass
    # define the fields for your item here like:
    # name = scrapy.Field()


class HouseKeepingItem(scrapy.Item):
    # housekeeping
    date = Field()
    source = Field()
    spider = Field()
    project = Field()
    server = Field()
    category = Field()


class DistrictItem(HouseKeepingItem):
    district = Field()
    subdistrict = Field()
    url = Field()


class PropertyItem(HouseKeepingItem):
    title = Field()
    url = Field()
    price = Field()
    address = Field()
    city_name = Field()
    dist_name = Field()
    subdist_name = Field()
    agent_name = Field()
    agent_company = Field()
    agent_phone = Field()
    station_name = Field()
    recent_activation = Field()


class IndexItem(HouseKeepingItem):
    url = Field()
    retrived = Field()


class AgentItem(HouseKeepingItem):
    name = Field()
    district = Field()
    subdistrict = Field()
    telephone = Field()
    history_amount = Field()
    recent_activation = Field()
    second_house_amount = Field()
    new_house_amount = Field()
    rent_house_amount = Field()
    company = Field()
    address = Field()
    register_date = Field()