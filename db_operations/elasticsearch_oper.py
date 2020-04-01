import elasticsearch
from elasticsearch import exceptions
from elasticsearch import helpers
import os
import ssl
import urllib3
import json
import configparser
import logging

logging.basicConfig(filename='elasticsearch.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

os.environ['NODE_TLS_REJECT_UNAUTHORIZED'] = '0'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


class Elastic_connect:
    def __init__(self, index):
        config = configparser.ConfigParser()
        configFilePath = r'C:\\bala\RNN_Models\pre_processors\db_operations\elastic_config.ini'
        config.read(configFilePath)
        self.index = index
        print(config.getboolean('ELASTICSEARCH', 'ssl'))
        if config.get('ELASTICSEARCH', 'ssl') == True:
            username = config['ELASTICSEARCH']['username']
            password = config['ELASTICSEARCH']['password']
            elastic_url = "https://" + username + ":" + password + "@" + config['ELASTICSEARCH']['host'] + ":" + \
                          config['ELASTICSEARCH']['port']
        else:
            elastic_url = "http://" + config['ELASTICSEARCH']['host'] + ":" + config['ELASTICSEARCH']['port']
        try:
            self.es = elasticsearch.Elasticsearch(elastic_url, ssl_context=ssl_context)
            print(elastic_url)
        except Exception as ex:
            logging.error("Problem in connecting elasticsearch - Elastic Error")

    def elastic_get_data(self, query):
        print(self.index)
        print(query)
        try:
            res = self.es.search(
                index=self.index,
                body=query)
            return res
        except Exception as ex:
            print(ex)
            logging.error("Problem in fetching index - Elastic Error")

    def ingest_es_data(self, index, data):
        try:
            for x in data:
                self.es.index(index=index, body=x, doc_type="_doc")
        except Exception as ex:
            logging.error("Problem in fetching index - Elastic Error")
