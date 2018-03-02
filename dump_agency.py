#!/usr/bin/env python
import sqlite3
import csv

f = open("data/agencies.tsv", "w", newline="")
csvWriter = csv.writer(f, dialect="excel", delimiter="\t")

with sqlite3.connect("data/esf_urls.db") as cnx:
    cursor = cnx.cursor()
    cursor.execute("""
        with t1 as (select *  from agencies where dist_name ='昆山' and name is not null),
        t2 as(select telephone,count(*) as cnt
              from t1
              GROUP BY telephone
              ORDER BY cnt desc
              LIMIT  25
             )
        SELECT t1.telephone,subdist_id, count(*), max(t2.cnt) from  t1 INNER JOIN t2 on t1.telephone = t2.telephone
        GROUP BY  t1.telephone, subdist_id
        order by max(t2.cnt) desc, count(*) desc
        ;
    """)
    rows = cursor.fetchall()

d = {}
for row in rows:
    d.setdefault((row[0], row[-1]), []).append(row[1])

for k, v in d.items():
    # make sure v is great or equal to 3
    if len(v) < 3:
        v = v[0:len(v)] + [0]*(3-len(v))
    row = (k[0], "20203%02d0022" %(v[0]))
    csvWriter.writerow(row)

with sqlite3.connect("data/esf_urls.db") as cnx:
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