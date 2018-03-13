#!/bin/bash
echo $(dirname $0)
echo $0

cd $(dirname $0)
. venv/bin/activate
today=`date +%Y%m%d`

scrapy crawl ${1} > log/${1}_${today}.log 2>&1

