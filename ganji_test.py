import time
import random
import csv
import re
import sqlite3

import requests
from requests import Session

from selenium.webdriver import Firefox
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import *
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from urllib.request import urljoin,urlparse

from bs4 import BeautifulSoup

from stem import Signal
from stem.control import Controller

class BasicSpider:
    total = 0
    name = 'ganji_esf'
    ip_url = "https://icanhazip.com"
    # start_url = 'http://sh.lianjia.com/ershoufang/'
    headers = {
        "USER-AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
    }
    def __init__(self,start_url,name,update=True):
        # initial db
        self._conect_db()
        self.session = self.get_session()
        self.name = name
        self.driver = self.get_driver()
        self.start_url = start_url
        self.file = open(name+".csv","a")
        self.csvWriter = csv.writer(self.file,delimiter='\t')

        # initial dist_name urls
        self.lvl1_urls = self._retrive("lvl1_urls",0)
        self.lvl1_urls_retrived = self._retrive("lvl1_urls", 1)

        self.lvl0_urls = self._retrive("lvl0_urls",0)
        self.lvl0_urls_retrived = self._retrive("lvl0_urls", 1)

        self.lvl0_urls_retrived.add(self.start_url)

        if update:
            self._get_start_urls(self.start_url)

    def scrapingAll(self):

        for url in self.lvl1_urls:
            self._insert("lvl1_urls",url)

        for url in self._retrive("lvl1_urls",0):
            self.parseCategroy(url)
            self._upd_retrive("lvl1_urls",url)

    def parseCategroy(self,url=None, overwrite=False):
        if not url:
            url = self.start_url
        self._sleep()

        self.req = self.session.get(url)
        # self.driver.get(url)
        # element = WebDriverWait(self.driver, 50).until(
        #     EC.presence_of_element_located((By.CLASS_NAME, "next"))
        # )
        #

        # # scrape data and record url
        for item in self.getItems():
            item.append(url)
            self.csvWriter.writerow(item)
        self.file.flush()
        self._upd_retrive("lvl1_urls", url)
        #
        #
        #
        # try:
        #     element = WebDriverWait(self.driver,10).until(
        #         EC.presence_of_element_located((By.XPATH,'//a[@class="next"]'))
        #     )
        #
        #     nexturl = urljoin(url, urlparse(element.get_attribute("href")).path)
        #
        #     # timeout = random.randint(1,100)
        #     # if timeout > 80:
        #     #     self.refreshDriver()
        #     print(nexturl)
        #     self.parseCategroy(nexturl)
        #
        # except Exception as e:
        #     pass
        # self.driver.close()

    def getItems(self):
        # ul = self.driver.find_element_by_xpath('//ul[@class="js_fang_list"]')
        bs = BeautifulSoup(self.req.text)
        dist_reg = re.compile("^不限$")
        district = bs.find("a",text=dist_reg).find_parent("ul").find("li",{"class":"item current"}).text
        subdist = bs.findAll("a",text=dist_reg)[1].find_parent("div").find("a",{"class":"subway-item current"}).text
        sub_r = re.compile("\\s+")
        id_reg = re.compile("^puid-\\d+")
        l = []
        for li in bs.findAll("div",{"id":id_reg}):
            prop_title = li.findAll("a")[1]
            title = prop_title.text.replace(" ","")
            url = urljoin(self.start_url, urlparse(prop_title["href"]).path)

            price = li.find("div",{"class":"price"}).text
            address = sub_r.sub("",li.find("span",{"class":"area"}).text)
            l.append([title,url,price,address,district,subdist])
        self.total += len(l)
        print("total current: ",self.total)
        return l

    def changeIP(self):
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)

    def get_session(self):
        session = Session()
        session.headers.update(self.headers)
        self.session = session
        return self.session

    def get_driver(self):
        profile = FirefoxProfile()
        profile.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36")
        return Firefox(profile)

    def refreshDriver(self):
        # make sure Tor and Proxies has installed and configure
        if hasattr(self,"driver"):
            self.driver.close()
        self.changeIP()

        proxy_address = "localhost:8118"
        proxy = Proxy()
        proxy.socksProxy = proxy_address

        profile = FirefoxProfile()
        proxy = Proxy({
            'proxyType':ProxyType.MANUAL,
            'httpProxy' : proxy_address,
            'httpsProxy': proxy_address,
            'ftpProxy': proxy_address,
            'sslProxy': proxy_address,
            'noProxy':""
        })
        profile.set_proxy(proxy)
        driver = Firefox(firefox_profile=profile)
        self.driver = driver

    def _get_start_urls(self,url):
        # get first dist_name level

        self.req = self.session.get(url)
        bs = BeautifulSoup(self.req.text)
        print(self.req.status_code)
        # WebDriverWait(self.driver, 3).until(EC.presence_of_element_located(
        #     (By.XPATH,'//div[@class="f-f-content"]')
        # ))

        self._get_level0_urls(bs)

        if len(self.lvl0_urls):
            new_url = self.lvl0_urls.pop()

            self._insert("lvl0_urls",new_url)

            print("new url: %s" %new_url)
            self.lvl0_urls_retrived.add(new_url)
            # filter first start url
            if url != self.start_url:
                self._get_level1_urls(bs)

            self._get_start_urls(new_url)

    def _get_level0_urls(self,bs):
        r = re.compile("^不限$")
        current = bs.find("a",text=r).find_parent("ul").find("li",{"class":"item current"}).text
        level0_urls = set([urljoin(self.start_url,urlparse(a.get("href")).path) for a in
                           bs.find("a", text=r).find_parent("ul").findAll("a")])
        self.lvl0_urls.update(level0_urls.difference(self.lvl0_urls_retrived))
        for url in self.lvl0_urls:
            print(url)
        # districts = self.driver.find_element_by_id("plateList")
        # for dist_name in districts.find_elements_by_xpath("//div[@class='level1']/a"):
        #     dist_url = urljoin(self.start_url,dist_name.get_attribute("href"))
        #     if dist_url not in self.lvl0_urls_retrived:
        #         level0_urls.add(dist_url)
        # self.lvl0_urls.update(level0_urls)
        # print("refresh level0 %s" % level0_urls)
        # return level0_urls
        return current

    def _get_level1_urls(self, bs):

        r = re.compile("^不限$")
        lvl1_urls = set([urljoin(self.start_url,a.get("href")) for a in
                          bs.find("a",{"class":"current"},text=r).find_parent("div").findAll("a")])
        # print(self._get_level0_urls(bs))
        # for url in lvl1_urls:
        #     print(url)
        lvl1_urls.difference_update(self.lvl0_urls_retrived)
        lvl1_urls.difference_update(self.lvl0_urls)
        self.lvl1_urls.update(lvl1_urls)
        # districts = self.driver.find_element_by_id("plateList")
        # level1_urls = set([i.get_attribute("href") for i in districts.find_elements_by_xpath("//div[@class='level2-item']/a")
        #  if i.get_attribute("href") not in self.lvl0_urls and self.lvl0_urls_retrived])
        # self.lvl1_urls.update(level1_urls)
        # return level1_urls

    def __del__(self):
        self.driver.close()
        self.file.close()
        if hasattr(self,"cnx"):
            self.cnx.close()

    def _sleep(self):
        time.sleep(random.randint(1,5))

    # database
    def _conect_db(self):
        cnx = sqlite3.connect("%s_urls.db" %self.name)
        self.cnx = cnx
        self.cursor = cnx.cursor()

    def _insert(self,table_name,url):
        if not self._exist(table_name,url):
            stmt = "insert into %s(url) values(?) " %table_name
            print("insert",stmt+url)
            self.cursor.execute(stmt,[url])
            self.cnx.commit()

    def _retrive(self,table_name, strat):
        stmt = "select url from %s where retrived = 0" %table_name
        self.cursor.execute(stmt)
        return set([item[0] for item in self.cursor.fetchall()])

    def _exist(self,tablename,url):
        stmt = "select url from %s where url = '%s'" %(tablename,url)
        self.cursor.execute(stmt)
        return len(self.cursor.fetchall()) > 0

    def _upd_retrive(self,table_name,url):
        stmt = "update %s set retrived = 1 where url = '%s'" %(table_name, url)
        self.cursor.execute(stmt)
        self.cnx.commit()


if __name__ == '__main__':
    spd = BasicSpider('http://sh.ganji.com/fang5/','ganji_esf',update=False)
    print(len(spd.lvl0_urls),spd.lvl0_urls)
    # spd.refreshDriver()
    spd.scrapingAll()