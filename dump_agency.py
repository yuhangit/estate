#!/usr/bin/env python
import pymysql
from esf.settings import MYSQL_PIPELINE_URL
import csv
import dj_database_url


def db_paras(url):
    paras = dj_database_url(MYSQL_PIPELINE_URL)
    db_paras = {}
    db_paras["host"] = paras["HOST"]
    db_paras["user"] = paras["USER"]
    db_paras["passwd"] = paras["PASSWORD"]
    db_paras["port"] = paras["PORT"]
    db_paras["db"] = paras["NAME"]
    db_paras = {k: v for k,v in db_paras.items() if v }
    return db_paras

f = open("data/agencies.tsv", "w", newline="")
csvWriter = csv.writer(f, dialect="excel", delimiter="\t")

with pymysql.connect(**db_paras(MYSQL_PIPELINE_URL)) as cnx:
    cursor = cnx.cursor()
    cursor.execute("""
        drop table if EXISTS agencies_unique;
    """)
    cursor.execute("""
        create table agencies_unique 
        as select distinct name,telephone,history_amount,recent_activation
            ,source,project, spider,server, dt, second_house_amount,new_house_amount, rent_house_
            amount, company,address, register_date, district_id, station_id,category_id 
            from agencies_temp;
    """)

rows = []
d = {}
for row in rows:
    d.setdefault((row[0], row[-1]), []).append(row[1])

for k, v in d.items():
    # make sure v is great or equal to 3
    if len(v) < 3:
        v = v[0:len(v)] + [0]*(3-len(v))
    row = (k[0], "20203%02d0022" %(v[0]))
    csvWriter.writerow(row)

with pymysql.connect(**db_paras(MYSQL_PIPELINE_URL)) as cnx:
    cursor = cnx.cursor()
    cursor.execute("""
      select telephone
      from main.agencies 
      where instr(source,'.fang.com') >0 and dist_name ='上海周边' and subdist_name= '昆山'
      ORDER BY second_house_amount DESC 
      LIMIT 25
""")
    rows = cursor.fetchall()

for row in rows:
    csvWriter.writerow([row[0], "20202030822"])