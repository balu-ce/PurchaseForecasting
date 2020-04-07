import json

from flask import Flask, request

from IngestionLayer.elastic_forecast_ingest import Elasticsearch_ingest
from Preprocessing_Scripts.denormalization import DeNormalization
from Preprocessing_Scripts.normalization import Normalization
from db_operations.elasticsearch_oper import Elastic_Sales_Connect
import numpy as np
import requests

app = Flask(__name__)

Tensorflow_Base_URL = "http://localhost:8501/v1/models/"

@app.route('/')
def hello():
    return "Hello World!"

@app.route('/forecast_ingest_pipeline')
def ingest_pipeline():
    interval = request.args.get('interval', default='monthly', type=str)
    es = Elastic_Sales_Connect()
    sales_stats_data = es.elastic_get_sales_stats()
    es_norm = Normalization(es.elastic_get_forecast_data())
    norm_value = es_norm.elasticsearch_forecast_normalize(sales_stats_data)
    norm_data = np.reshape(norm_value, (len(norm_value), 1))
    data = {"instances": [norm_data.tolist()]}
    sales_forecast_pred_url = Tensorflow_Base_URL + 'sales_forecast:predict'
    r = requests.post(url=sales_forecast_pred_url, data=json.dumps(data))
    pred_val = json.loads(r.text)['predictions'][0]
    de_norm_obj = DeNormalization(pred_val)
    de_norm_value = de_norm_obj.elasticsearch_forecast_denormalize(sales_stats_data)
    es_ingest_obj = Elasticsearch_ingest(de_norm_value)
    ingest_val = es_ingest_obj.forecast_ingest_fn(interval)
    es.ingest_es_forecast_data(ingest_val)
    return "Data Ingested Successfully"

if __name__ == '__main__':
    app.run(debug=True)
