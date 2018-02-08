import requests
from bs4 import BeautifulSoup
import re
import csv
import time
import sys

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"}

#store_reg = re.compile(r"name:'(.*)'")
#para_reg = re.compile(r"para:'([^,]*)'")
#mobile_reg = re.compile(r"mobile:'([^,]*)'")
#name_reg = re.compile(r"cnName:'(.*)'")
#name_reg= (r"name:'(.*)'")
#mobile_reg=re.compile(r"mobile:'([^,]*)'")
#store_reg=re.compile(r"name:'(.*)'")
#store_reg=re.compile(r"name:'(.*)'")
#district_reg=re.compile(r"name:'(.*)'")
#sub_district_reg=re.compile(r"name:'(.*)'")

def extractOrDefault(ls,default="undefined"):
    if len(ls) >= 1:
        return ls[0]
    return default

def getItems(writer,url):
    req = requests.get(url, headers=headers)
    if req.status_code == 200:
        bs = BeautifulSoup(req.text)
        #listItems = bs.findAll("li",{"class":"clearfix js_point_list"})
        #listItems = bs.findAll("div",{"class":"broker-cont fl-l"})
        listItems = bs.findAll("div",{"class":"f-list-item"})
        #input()
        print("url: %s" %url)
        print("total items:%s" %len(listItems))
        for items in listItems:
            #print(store_reg.findall(items.get("zvalue").replace('"',"'")))
            #store = extractOrDefault(store_reg.findall(items.get("zvalue").replace('"',"'")))
            #para = extractOrDefault(para_reg.findall(items.get("zvalue").replace('"', "'")))
            #mobile =  extractOrDefault(mobile_reg.findall(items.find("p",{"class":"phone"}).find("b").get("zvalue").replace('"', "'")))
            #name =  extractOrDefault(name_reg.findall(items.find("p",{"class":"phone"}).find("b").get("zvalue").replace('"', "'")))      
            Agent = items.find("a",{"class":"broker-name"}).text
            print("agent is ok:%s" %Agent)
            Store = items.find("span",{"class":"bi-text broker-company"}).text
            print("store is ok:%s" %Store)
            Telephone = items.find("p",{"class":"tel"}).text
            print("Telephone is ok:%s" %Telephone)
            area = [i.text for i in items.findAll("span",{"class":"bi-text"})][1].split("-")
            District = area[0]
            print("District is ok:%s" %District)
            sub_District =""
            if len(area)>1:
                sub_District = area[1]
            print("District is ok:%s" %sub_District)
            
            #Sub_District = "".join(items.find("span",{"class":"bi-text"}).findAll("href=")[2].text.split())
            #secondHouse = "".join(items.find("div",{"class":"outstanding"}).find("p").text.split())
            #rentHouse = "".join(items.find("div",{"class":"outstanding"}).findAll("p")[1].text.split())
            #visitedCount = "".join(items.find("div",{"class":"outstanding"}).findAll("p")[2].text.split())
            #row = [store,para,mobile,name,secondHouse,rentHouse,visitedCount]
            row = [Agent,Store,Telephone,District,sub_District]
            writer.writerow(row)
            
    else:
        print("url: %s requests status %s, retry again" %(url,req.status_code))


def main():
    # url = "http://sh.centanet.com/jingjiren/"
    url = "http://sh.ganji.com/fang/agent/"
    allOrOne = input("one or all: ")
    startPage = int(input("start page: "))
    if allOrOne == "all":
        urls = [ url+ 'o%s/' %i for i in range(startPage,51)]
    else:
        urls = [url +'o%s/' % startPage]
    f = open("ganji.txt", "a")

    csvWriter = csv.writer(f)

    try:
        for url in urls:
            getItems(csvWriter,url)
            time.sleep(10)
            f.flush()
    except Exception as e:
        print(sys.exc_info())
        print("stop at url: %s" % url)
        f.close()

if __name__ == "__main__":
    main()