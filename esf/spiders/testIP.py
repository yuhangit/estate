# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request

class TestipSpider(scrapy.Spider):
    name = 'testIP'
    allowed_domains = ['icanhazip.com']
    start_urls = ['http://icanhazip.com/']

    def parse(self, response):
        i = {}
        i['ip'] = response.text
        yield i
        yield Request(response.url,dont_filter=True)
