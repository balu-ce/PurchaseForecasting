import datetime
from dateutil.relativedelta import *


class Elasticsearch_ingest:
    def __init__(self, data):
        self.data = data

    def forecast_ingest_fn(self, interval):
        return getattr(self, 'case_' + interval)()

    def case_monthly(self):
        arr_to_ingest = list()
        for i in range(len(self.data)):
            dict = {}
            dt = datetime.datetime.now()
            dt = dt + relativedelta(months=+i)
            dt = dt.replace(day=1)
            dict['date'] = dt
            dict['forecast_value'] = self.data[i]
            dict['interval'] = "monthly"
            arr_to_ingest.append(dict)
        
        return arr_to_ingest
