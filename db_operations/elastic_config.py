import configparser
config = configparser.ConfigParser()
configFilePath = 'elastic_config.ini'
config.read(configFilePath)

class Elastic_Index_Config:
    @staticmethod
    def get_sales_index():
        return config['ELASTICSEARCH']['sales_data_index']

    @staticmethod
    def get_sales_forecast_index():
        return config['ELASTICSEARCH']['forecast_index']

    @staticmethod
    def get_sales_rfm_index():
        return config['ELASTICSEARCH']['rfm_index']

    @staticmethod
    def get_business_unit_key():
        return config['ELASTICSEARCH']['business_unit']
