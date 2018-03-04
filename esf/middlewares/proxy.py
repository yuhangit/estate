# -*- coding: utf-8 -*-
import random
import logging
import urllib
import requests
from bs4 import BeautifulSoup
import time
import sqlite3
import traceback

try:
    from stem import Signal
    from stem.control import Controller
except:
    print("lack of stem library for TorProxyMiddleware, install first ")

from scrapy.utils.project import get_project_settings
from scrapy.http import Request
from twisted.internet.error import TimeoutError
from scrapy.exceptions import IgnoreRequest


class TorProxyMiddleware(object):
    def __init__(self):
        self._import_setting()
        self.ip = self.get_ip()
        self.req_counter = 0

    def get_ip(self):
        url = "http://icanhazip.com"
        return requests.get(url,proxies=self.requests_proxy).text.strip()

    def _import_setting(self):
        settings = get_project_settings()
        self.http_proxy = settings["HTTP_PROXY"]
        self.tor_password = settings["PASSWORD"]
        self.control_port = settings["CONTROL_PORT"]
        self.max_req_per_ip = settings["MAX_REQ_PER_IP"]
        self.requests_proxy = { "http": self.http_proxy,
                                "https": self.http_proxy}
        # nodes that singal to exits tor
        self.exit_nodes = settings["EXIT_NODES"]
        if self.exit_nodes:
            with Controller.from_port(port=self.control_port) as controller:
                controller.authenticate()
                controller.set_conf('ExitNodes',self.exit_nodes)

    def change_ip(self):
        with Controller.from_port(port=self.control_port) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
        return self.get_ip()

    def process_request(self, request, spider):
        self.req_counter += 1
        if self.max_req_per_ip is not None and self.req_counter > self.max_req_per_ip:
            i = 1
            while 1:
                ip = self.change_ip()
                i += 1
                if ip != self.ip or i > 10:
                    self.ip = ip
                    break
        request.meta['proxy'] = self.http_proxy
        logging.info("Using proxy: %s" % request.meta["proxy"])


class HTTPProxyMiddleware(object):
    proxies = []
    max_proxies = 10000

    # for proxy
    start_page = 1
    end_page = 10

    headers = {
        "User-Agent":get_project_settings()["USER_AGENT"]
    }

    def __init__(self):
        self.time = time.time()
        self.loger = logging.getLogger(__file__)
        self.query_proxies()


    def query_proxies(self):
        api = get_project_settings().get("PROXY_API")
        urls = [ 'http://%s' % ip for ip in requests.get(api,headers=self.headers).text.split()]
        # urls = [i.strip() for i in open("proxies.txt").readlines()]
        for url in urls:
            # req = requests.get(url,headers = self.headers)
            # if req.status_code == 200:

                # bs = BeautifulSoup(req.text, 'html.parser')
            #     for tr in bs.findAll("tr"):
            #         cells = tr.findAll("td")
            #         if len(cells) == 7:
            #             proxy = cells[3].text + "://" + cells[0].text+ ":" + cells[1].text
            #             self.proxies.append(proxy)
            #             logging.info("add proxy: %s" % proxy)
            # req.close()
            if url not in self.proxies:
                self.proxies.append(url)
        # self.start_page = self.end_page
        # self.end_page += 10

    def process_request(self, request, spider):
        # if time.time() - self.time > 600 and time.time() - self.time > 5: # api restrict
        #     self.loger.info("add new proxies")
        #     self.time = time.time()
        #     self.proxies.clear()
        #     self.query_proxies()
        #     self.loger.info("%d proxies now " %len(self.proxies))

        # remove refer in headers
        # if "Referer" in request.headers:
        #     request.headers.pop("Referer")
        # self.loger.critical("request <%s> headers %s",request.url,request.headers)

        # fix broken url:
        # 关闭了301 redirect, 5i5j 需要手动添加 /
        if '.5i5j.com' in request.url and not request.url.endswith('/'):
            self.loger.info("request url <%s> not endswith '/'", request.url)
            request = request.replace(url=request.url + "/")
            # reprocess the request
            return request

        if "proxy" in request.meta:
            self.loger.critical("request <%s> has proxy already, remove it", request.url)
            self.remove_failed_proxy(request,spider)
        # else:
        #     self.loger.critical("url %s has no proxy.", request.url)

        if "timeout_retry" in request.meta:
            self.loger.critical("request url %s has timeout: %s", request.url,
                                request.meta.get("timeout_retry", 0))

        proxy = random.choice(self.proxies)
        request.meta['proxy'] = proxy
        #self.loger.critical('url: %s Using proxy: %s',request.url, request.meta['proxy'])

    def remove_failed_proxy(self, request, spider):
        failed_proxy = request.meta['proxy']
        logging.log(logging.DEBUG, 'Removing failed proxy...')
        try:
            i = 0
            for proxy in self.proxies:
                if proxy in failed_proxy:
                    del self.proxies[i]
                    proxies_num = len(self.proxies)
                    self.loger.info(
                        'Removed failed proxy <%s>, %d proxies left', failed_proxy, proxies_num)
                    if proxies_num < 50:
                        self.query_proxies()
                    return True
                i += 1
        except KeyError:
            logging.log(logging.ERROR, 'Error while removing failed proxy')
        return False

    def process_exception(self, request, exception, spider):
        # if request.url.startswith("http://10.") or getattr(request.meta,'cnt', 0) > 10:
        #     logging.info("request's url is <%s> and had retry %d times", request.url, request.meta.get('cnt',0))
        #     return None
        # else:
        #     if self.remove_failed_proxy(request, spider):
        #         request.meta['cnt'] = request.meta.get('cnt', 0) + 1
        #         logging.info("exception happened")
        #         return request
        if isinstance(exception, IgnoreRequest):
            self.loger.error("Ignore Request <%s>", request.url)
            return None
        if request.url.startswith("http://10.") :
            return None

        self.loger.info("exception in <%s>:%s",request.url ,str(exception))
        if isinstance(exception,TimeoutError):
            # self.loger.info("timeout error happened, retry: %s" % request.url)
            request.meta["timeout_retry"] = request.meta.get("timeout_retry", 0) + 1

        if self.remove_failed_proxy(request, spider):
            return request
        return request

    def process_response(self, request, response, spider):
        # really brutal filter
        if response.status == 200:
            return response
        # request.meta['cnt'] = request.meta.get('cnt', 0) + 1
        self.loger.info("%s request status %s, proxy: %s." %(request.url, response.status, request.meta.get("proxy")))
        return request