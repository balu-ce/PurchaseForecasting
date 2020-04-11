import time

from Preprocessing_Scripts.RFM_calculation import RFM_Analysis
from app import celery
from db_operations.elasticsearch_oper import Elastic_Sales_Connect


@celery.task(bind=True)
def rfm_analysis(self):
    self.update_state(state='PROGRESS', meta={'status': 'Collecting Sales Data'})
    es = Elastic_Sales_Connect()  # setup a elasticsearch connection
    sales_data = [hit["_source"] for hit in es.elastic_get_sales_data()]
    self.update_state(state='PROGRESS', meta={'status': 'Processing Customer Segmentation'})
    rfm_data = RFM_Analysis(sales_data)
    time.sleep(40)
    data = rfm_data.rfm_calc()
    self.update_state(state='PROGRESS', meta={'status': 'Refreshing Customer Index'})
    es.ingest_es_rmf_data(data)
    return {'status': 'Customer List Ingestion completed!'}

