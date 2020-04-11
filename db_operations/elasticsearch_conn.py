import configparser
import os
import ssl
import elasticsearch
import urllib3
from loggers.es_logger import ES_Logger

os.environ['NODE_TLS_REJECT_UNAUTHORIZED'] = '0'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
config = configparser.ConfigParser()
configFilePath = 'elastic_config.ini'
config.read(configFilePath)


class Elastic_Conn_Sales_data:

    @staticmethod
    def elastic_sales_conn():
        if config.getboolean('ELASTICSEARCH', 'ssl'):
            username = config['ELASTICSEARCH']['username']
            password = config['ELASTICSEARCH']['password']
            elastic_url = "https://" + username + ":" + password + "@" + config['ELASTICSEARCH']['host'] + ":" + \
                          config['ELASTICSEARCH']['port']
        else:
            elastic_url = "http://" + config['ELASTICSEARCH']['host'] + ":" + config['ELASTICSEARCH']['port']
        es = elasticsearch.Elasticsearch(elastic_url, ssl_context=ssl_context)
        try:
            es.ping()
        except Exception as ex:
            print(ex)
            ES_Logger.error_logs("Error in connecting to Elasticsearch")
        return es
