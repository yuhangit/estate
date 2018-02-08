# -*- coding: utf-8 -*-

# Scrapy settings for esf project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'esf'

SPIDER_MODULES = ['esf.spiders']
NEWSPIDER_MODULE = 'esf.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 1

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.1
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = True

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'esf.middlewares.EsfSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   #'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    # 'esf.middlewares.proxy.TorProxyMiddleware' : 400,
   # 'scrapy.downloadermiddlewares.retry.RetryMiddleware':None, # this middleware used to retry returned requsts
    'esf.middlewares.proxy.HTTPProxyMiddleware': 400,
   # 'esf.middlewares.CustomRetry.CustomRetryMiddleware': 500,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware':None,
    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware':None,
    "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware":None
    #'esf.middlewares.user_agent.RandomUserAgentMiddleware': 300
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'esf.pipelines.SqlitePipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# maximum number of times to retry
RETRY_ITEMS = 5
# HTTP response codes to retry
RETRY_HTTP_CODES = [500, 502, 503, 504, 302, 301, 404, 401, 402, 403, 307]
DOWNLOAD_TIMEOUT = 5


# logging
#LOG_FILE = "esf.log"
LOG_LEVEL = "INFO"
#######  user define area  #####
# tor setting
HTTP_PROXY = "http://127.0.0.1:8118"
CONTROL_PORT = 9051
TOR_PASSWORD = '16:81270390EE65C6F1606CBFF160A272B91884D1106963ACF2BC872EBBC2'
#
MAX_REQ_PER_IP = 1000

# used for requests
REQUESTS_HEADERS = {
    "User-Agent":'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'
}

# user for store data spider_name + STORE_DATABASE_BASE
STORE_DATABASE = "data/esf_urls.db"