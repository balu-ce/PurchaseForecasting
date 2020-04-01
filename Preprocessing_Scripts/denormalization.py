
class deNormalization:
    def __init__(self, data):
        self.data = data

    def elasticsearch_forecast_denormalize(self, stats_data):
        """
            Function is used for de-normalizing a dataset coming from a data source of elasticsearch
        """
        de_norm: [] = self.data
        mean = stats_data['aggregations']['stats_sales']['avg']
        std_dev = stats_data['aggregations']['stats_sales']['std_deviation']
        for i in range(len(de_norm)):
            """ 
                De-Normalizing a data using z-score technique((data*standard_deviation)+mean)
            """
            de_norm[i] = (de_norm[i]*std_dev)+mean
        return de_norm
