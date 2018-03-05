#!/usr/bin/env python
import pymysql
from esf.settings import MYSQL_PIPELINE_URL
import csv
import dj_database_url
import sys

datatype = 1
accuracy = 0

if len(sys.argv) > 1:
    datatype = sys.argv[1]
elif len(sys.argv)> 2:
    accuracy = sys.argv[2]

print("数据端口：%s, 精确度：%s" %(datatype, accuracy))

def db_paras(url):
    paras = dj_database_url.parse(MYSQL_PIPELINE_URL)
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

with pymysql.connect(**db_paras(MYSQL_PIPELINE_URL)) as cursor:
    cursor.execute("""
        drop table if EXISTS estate.agencies_unique;
    """)
    cursor.execute("""
        create table estate.agencies_unique 
        as select distinct name,telephone,history_amount,recent_activation
            ,source,project, spider,server, dt, second_house_amount,new_house_amount, rent_house_amount
            , company,address, register_date, district_id, station_id,category_id 
            from estate.agencies_temp;
    """)
    cursor.execute("select city_id,dist_id, subdist_id, station_id, category_id,telephone from estate.v_agencies_top50")
    rows = cursor.fetchall()


for row in rows:
    format_row = list(row[:-1])
    format_row.insert(0, datatype)
    format_row.append(accuracy)
    row = ( row[-1], '%d%02d%02d%02d%02d%d%d' %tuple(format_row))
    csvWriter.writerow(row)
