import sqlite3
import csv

f = open("data/agencies.tsv", "w", newline="")
csvWriter = csv.writer(f, dialect="excel", delimiter="\t")

with sqlite3.connect("data/esf_urls.db") as cnx:
    cursor = cnx.cursor()
    cursor.execute("""
    with t1 as (select rowid,*  from agencies where district ='昆山' and name is not null),
    t2 as(select name,telephone,count(*) as cnt, group_concat(DISTINCT subdist_id)
      from t1
      GROUP BY name,telephone
      HAVING count(*) > 50)
    SELECT agencies.telephone, agencies.dist_id,agencies.subdist_id
      ,t2.cnt
    from agencies
      INNER JOIN  t2 on  agencies.telephone = t2.telephone
    where dist_id = 99  
    """)
    rows = [(row[0],"%02d%02d%03d" %(row[1],row[2],row[3]))  for row in cursor.fetchall()]

csvWriter.writerows(rows)



