from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
import logging


class CustomRetryMiddleware(RetryMiddleware):
    def __init__(self,*args,**kwargs):
        super(CustomRetryMiddleware,self).__init__(*args,**kwargs)
        self.loger = logging.getLogger(__file__)

    def process_response(self,request,response,spider):
        if request.meta.get("dont_retry",False):
            return response

        if response.status in self.retry_http_codes:
            self.loger.info("request url: %s ,response status %s, max try time:%s , have done: %s"
                            % (request.url, response.status
                            ,self.max_retry_times, request.meta.get("retry_times",0)))

            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or request
        return response

    # def process_exception(self, request, exception, spider):
    #     self.loger.info("exception happend %s, retry url: %s" % (exception ,request.url))
    #     self.loger.info("proxy: %s" % request.meta.get("proxy", "None"))
    #     return request