# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.mail import MailSender
from scrapy import signals

class TestipSpider(scrapy.Spider):
    name = 'testIP'
    allowed_domains = ['icanhazip.com']
    start_urls = ['http://icanhazip.com/']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = cls(*args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def parse(self, response):
        i = {}
        i['ip'] = response.text
        yield i
        yield Request(response.url,dont_filter=True)

    def spider_closed(self, spider):
        mailer = MailSender(mailfrom="gufengxiaoyuehan@gamil.com", smtphost="smtp.gmail.com",
                            smtpport=22, smtpuser="logan@gufengxiaoyuehan.xyz",smtppass="L_c09010163")

        return mailer.send(to="gufengxiaoyuehan@163.com",subject="test",body="body")