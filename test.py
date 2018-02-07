import time
import random
import csv

import sqlite3

from selenium.webdriver import Firefox
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import *
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from urllib.request import urljoin

from bs4 import BeautifulSoup

from stem import Signal
from stem.control import Controller

class BasicSpider:
    total = 0
    name = 'basic'
    ip_url = "https://icanhazip.com"
    # start_url = 'http://sh.lianjia.com/ershoufang/'

    def __init__(self,start_url,filename):
        self.driver = Firefox()
        self.start_url = start_url
        self.file = open(filename,"w")
        self.csvWriter = csv.writer(self.file,delimiter='\t')

        # initial district urls
        self.lvl1_urls = set()
        self.lvl1_urls_retrived = set()
        self.lvl0_urls = set()
        self.lvl0_urls_retrived = set()
        self.lvl0_urls_retrived.add(self.start_url)
        self._get_start_urls(self.start_url)

    def scrapingAll(self):
        for url in self.lvl1_urls:
            self.parseCategroy(url)

    def parseCategroy(self,url=None):
        if not url:
            url = self.start_url
        self._sleep()
        self.driver.get(url)
        element = WebDriverWait(self.driver, 50).until(
            EC.presence_of_element_located((By.CLASS_NAME, "c-pagination"))
        )

        for item in self.getItems():
            item.append(url)
            self.csvWriter.writerow(item)
        self.file.flush()
        # current = self.driver.find_element_by_xpath('//*[@class="c-pagination"]/'
        #                                             'a[@class="current"]')
        next = self.driver.find_elements_by_xpath('//*[@class="c-pagination"]/'
                                                 'a[@gahref="results_next_page"]')
        # currenturl = urljoin(url, current.get_attribute("href"))
        if len(next) == 1:
            nexturl = urljoin(url, next[0].get_attribute("href"))

            # timeout = random.randint(1,100)
            # if timeout > 80:
            #     self.refreshDriver()
            print(nexturl)
            self.parseCategroy(nexturl)
        else:
            print("in parseCategroy: %d next find, url -> %s" %(len(next),url))
        # self.driver.close()

    def getItems(self):
        # ul = self.driver.find_element_by_xpath('//ul[@class="js_fang_list"]')
        bs = BeautifulSoup(self.driver.page_source)
        l = []
        for li in bs.find("ul",{"class":"js_fang_list" }).findAll("li"):
            prop_title  = li.find("div",{"class":"prop-title"}).find("a")
            title = prop_title.text.replace(" ","")
            url = urljoin(self.start_url,prop_title["href"])

            infos = li.findAll("div",{"class" :"info-row"})
            price = infos[0].find("div",{"class":"info-col price-item main"}).text.replace("\n","")

            address = ":".join([tag.text for tag in infos[1].findAll("a")])
            l.append([title,url,price,address])
        self.total += len(l)
        print("total current: ",self.total)
        return l

    def changeIP(self):
        with Controller.from_port(port=9051) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)

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
        # get first district level
        self._sleep()
        self.driver.get(url)
        self._get_level0_urls()

        if len(self.lvl0_urls):
            new_url = self.lvl0_urls.pop()
            print("new url: %s" %new_url)
            self.lvl0_urls_retrived.add(new_url)
            self._get_level1_urls()
            self._get_start_urls(new_url)

    def _get_level0_urls(self):
        level0_urls = set()
        districts = self.driver.find_element_by_id("plateList")
        for district in districts.find_elements_by_xpath("//div[@class='level1']/a"):
            dist_url = urljoin(self.start_url,district.get_attribute("href"))
            if dist_url not in self.lvl0_urls_retrived:
                level0_urls.add(dist_url)
        self.lvl0_urls.update(level0_urls)
        print("refresh level0 %s" % level0_urls)
        return level0_urls

    def _get_level1_urls(self):
        districts = self.driver.find_element_by_id("plateList")
        level1_urls = set([i.get_attribute("href") for i in districts.find_elements_by_xpath("//div[@class='level2-item']/a")
         if i.get_attribute("href") not in self.lvl0_urls and self.lvl0_urls_retrived])
        self.lvl1_urls.update(level1_urls)
        return level1_urls

    def __del__(self):
        self.driver.close()
        self.file.close()

    def _sleep(self):
        time.sleep(random.randint(1,5))

    def _conect_db(self):
        cnx = sqlite3.connect("urls.db")
        self.cnx = cnx
        self.cursor = cnx.cursor()
    def _insert(self,table_name,url):
        stmt = "insert into %s(url) values(?) " %table_name
        self.cursor.execute(stmt,[url])
        self.cnx.commit()
    def _retrive(self,table_name):
        stmt = "select url from %s" %table_name
        self.cursor.execute(stmt)
        return set([item[0] for item in self.cursor.fetchall()])
    def _exist(self,tablename,url):
        stmt = "select url from %s where url = %s" %(tablename,url)
        self.cursor.execute(stmt)
        return len(self.cursor.fetchall()) > 0

if __name__ == '__main__':
    spd = BasicSpider('http://sh.lianjia.com/ershoufang/','ershoufang.txt')
    print(len(spd.lvl1_urls),spd.lvl1_urls)
    # spd.refreshDriver()
    spd.scrapingAll()