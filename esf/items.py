# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class HouseKeepingItem(scrapy.Item):
    # housekeeping
    dt = Field()
    source = Field()
    spider = Field()
    project = Field()
    server = Field()

    category = Field()
    city_name = Field()
    dist_name = Field()
    subdist_name = Field()
    station_name = Field()


class DistrictItem(HouseKeepingItem):
    url = Field()


class PropertyItem(HouseKeepingItem):
    title = Field()
    url = Field()
    price = Field()
    address = Field()

    agent_name = Field()
    agent_company = Field()
    agent_phone = Field()
    recent_activation = Field()


class IndexItem(HouseKeepingItem):
    url = Field()


class AgentItem(HouseKeepingItem):
    name = Field()
    telephone = Field()
    history_amount = Field()
    recent_activation = Field()
    second_house_amount = Field()
    new_house_amount = Field()
    rent_house_amount = Field()
    company = Field()
    address = Field()
    register_date = Field()