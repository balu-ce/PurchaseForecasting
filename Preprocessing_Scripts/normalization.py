class normalization:
    def __init__(self, data):
        self.data = data

    def elasticsearch_forecast_normalize(self, stats_data):
        """
            Function is used for normalizing a dataset coming from a data source of elasticsearch
        """
        norm_arr = list()
        temp = self.data['aggregations']['sales_per_month']['buckets']
        mean = stats_data['aggregations']['stats_sales']['avg']
        std_dev = stats_data['aggregations']['stats_sales']['std_deviation']
        temp.pop(0)
        for i in reversed(temp):
            """ 
                Normalizing a data using z-score technique((data-mean)/standard_deviation)
            """
            norm_arr.append((i['sales_sum']['value'] - mean) / std_dev)
        return norm_arr
