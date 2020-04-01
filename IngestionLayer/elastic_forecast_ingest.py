import datetime
from dateutil.relativedelta import *


class elasticsearch_ingest:
    def __init__(self, data):
        self.data = data

    def forecast_ingest_fn(self, interval):
        return getattr(self, 'elasticsearch_ingest.case_' + interval)()

    def case_monthly(self):
        arr_to_ingest = list()

        for i in range(len(self.data) - 1):
            ingest_obj = {"date": "new Date()", "forecast_value": ""}
            dt = datetime.datetime.now()
            dt = dt + relativedelta(months=+i)
            dt = dt.replace(day=1)
            dict['date'] = dt
            dict['forecast_value'] = self.data[i]
            arr_to_ingest.append(dict)
        
        return arr_to_ingest
