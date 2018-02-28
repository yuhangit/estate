#!/bin/bash
echo $(dirname $0)
echo $0

cd $(dirname $0)
. venv/bin/activate

scrapy crawl KunShanAllScrapeSpider
