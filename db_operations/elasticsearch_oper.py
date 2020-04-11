from QueryBuilder.sales_forecast_builder import Sales_Query_Builder
from db_operations.elastic_config import Elastic_Index_Config
from db_operations.elasticsearch_conn import Elastic_Conn_Sales_data
from loggers.es_logger import ES_Logger
from elasticsearch import helpers

class Elastic_Sales_Connect:
    def __init__(self):
        self.es = Elastic_Conn_Sales_data.elastic_sales_conn()

    def elastic_get_forecast_data(self):
        try:
            res = self.es.search(
                index=Elastic_Index_Config.get_sales_index(),
                body=Sales_Query_Builder.get_forecast_query())
            return res
        except Exception as ex:
            ES_Logger.error_logs(str(ex))

    def elastic_get_sales_data(self):
        try:
            res = list(helpers.scan(self.es, size=10000, scroll='1m', query=Sales_Query_Builder.get_all_sales_query()))
            return res
        except Exception as ex:
            ES_Logger.error_logs(str(ex))

    def elastic_get_sales_stats(self):
        try:
            res = self.es.search(
                index=Elastic_Index_Config.get_sales_index(),
                body=Sales_Query_Builder.get_avg_query())
            return res
        except Exception as ex:
            ES_Logger.error_logs(str(ex))

    def ingest_es_forecast_data(self, data):
        try:
            for x in data:
                self.es.index(index=Elastic_Index_Config.get_sales_forecast_index(), body=x, doc_type="_doc")
        except Exception as ex:
            ES_Logger.error_logs(str(ex))

    def ingest_es_rmf_data(self, data):
        try:
            bulk_data = [
                {
                    "_index": Elastic_Index_Config.get_sales_rfm_index(),
                    "_type": "_doc",
                    "_id": str(source['Customer_ID']),
                    "_source": source
                }
                for source in data
            ]
            helpers.bulk(self.es, bulk_data)
            return "Ingested Successfully"

        # for x in data:
        #    self.es.index(index=Elastic_Conn_Sales_data.get_sales_rfm_index(), body=x, id=x['Customer_ID'], doc_type="_doc")
        except Exception as ex:
            ES_Logger.error_logs(str(ex))
