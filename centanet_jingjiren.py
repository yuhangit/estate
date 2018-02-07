import requests
from bs4 import BeautifulSoup
import re
import csv
import time

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"}

store_reg = re.compile(r"name:'(.*)'")
para_reg = re.compile(r"para:'([^,]*)'")
mobile_reg = re.compile(r"mobile:'([^,]*)'")
name_reg = re.compile(r"cnName:'(.*)'")

url = "http://sh.centanet.com/jingjiren/"
allOrOne = input("one or all: ")
startPage = int(input("start page: "))
if allOrOne == "all":
    urls = [ url+ 'g%s/' %i for i in range(startPage,340)]
else:
    urls = [url + 'g%s/' % startPage]
f = open("centanet_jingjiren.txt", "a",encoding='utf-8-sig')

csvWriter = csv.writer(f)

session = requests.session()

def extractOrDefault(ls,default="未定义"):
    if len(ls) >= 1:
        return ls[0]
    return default

def getItems(url):
    req = session.get(url, headers=headers)
    if req.status_code == 200:
        bs = BeautifulSoup(req.text)
        listItems = bs.findAll("li",{"class":"clearfix js_point_list"})
        print("url: %s" %url)
        print("total items:%s" %len(listItems))
        for items in listItems:
            print(store_reg.findall(items.get("zvalue").replace('"',"'")))
            store = extractOrDefault(store_reg.findall(items.get("zvalue").replace('"',"'")))
            para = extractOrDefault(para_reg.findall(items.get("zvalue").replace('"', "'")))
            mobile =  extractOrDefault(mobile_reg.findall(items.find("p",{"class":"phone"}).find("b").get("zvalue").replace('"', "'")))
            name =  extractOrDefault(name_reg.findall(items.find("p",{"class":"phone"}).find("b").get("zvalue").replace('"', "'")))
            secondHouse = "".join(items.find("div",{"class":"outstanding"}).find("p").text.split())
            rentHouse = "".join(items.find("div",{"class":"outstanding"}).findAll("p")[1].text.split())
            visitedCount = "".join(items.find("div",{"class":"outstanding"}).findAll("p")[2].text.split())
            row = [store,para,mobile,name,secondHouse,rentHouse,visitedCount]
            csvWriter.writerow(row)
            f.flush()
    else:
        print("url: %s requests status %s, retry again" %(url,req.status_code))


for url in urls:
    getItems(url)
    time.sleep(10)

f.close()