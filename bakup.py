import sqlite3
import time
import os
from shutil import copyfile
src = 'data/esf_urls.db'
db_bakup = "data/esf_urls_%s.db" % time.strftime("%Y%m%d")
# copy
copyfile(src,db_bakup)

with sqlite3.connect(src) as cnx:
    cursor = cnx.cursor()
    cursor.execute("DELETE from main.district")
    cursor.execute("DELETE from main.agencies")
    cursor.execute("DELETE from main.index_pages")
    cursor.execute("DELETE FROM main.properties")
    cnx.commit()
