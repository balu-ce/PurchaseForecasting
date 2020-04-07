import os
import ssl
import urllib3

from QueryBuilder.sales_forecast_builder import Sales_Query_Builder
from db_operations.elasticsearch_conn import Elastic_Conn_Sales_data
from loggers.es_logger import ES_Logger

os.environ['NODE_TLS_REJECT_UNAUTHORIZED'] = '0'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


class Elastic_Sales_Connect:
    def __init__(self):
        self.es = Elastic_Conn_Sales_data.elastic_sales_conn()

    def elastic_get_forecast_data(self):
        try:
            res = self.es.search(
                index=Elastic_Conn_Sales_data.get_sales_index(),
                body=Sales_Query_Builder.get_forecast_query())
            return res
        except Exception as ex:
            ES_Logger.error_logs(ex)

    def elastic_get_sales_stats(self):
        try:
            res = self.es.search(
                index=Elastic_Conn_Sales_data.get_sales_index(),
                body=Sales_Query_Builder.get_avg_query())
            return res
        except Exception as ex:
            ES_Logger.error_logs(ex)

    def ingest_es_forecast_data(self, data):
        try:
            for x in data:
                self.es.index(index=Elastic_Conn_Sales_data.get_sales_forecast_index(), body=x, doc_type="_doc")
        except Exception as ex:
            ES_Logger.error_logs(ex)
