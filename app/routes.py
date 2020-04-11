import json
from flask import Blueprint, request, jsonify, url_for
import numpy as np
from IngestionLayer.elastic_forecast_ingest import Elasticsearch_ingest
from Preprocessing_Scripts.denormalization import DeNormalization
from Preprocessing_Scripts.normalization import Normalization
import requests

from app.tasks import rfm_analysis
from db_operations.elasticsearch_oper import Elastic_Sales_Connect
from loggers.es_logger import ES_Logger

bp = Blueprint("sales_analysis", __name__)

Tensorflow_Base_URL = "http://localhost:8501/v1/models/"


@bp.route('/')
def hello():
    return "Hello World!"


@bp.route('/forecast_ingest_pipeline')
def ingest_pipeline():
    interval = request.args.get('interval', default='monthly', type=str)
    es = Elastic_Sales_Connect()  # setup a elasticsearch connection
    sales_stats_data = es.elastic_get_sales_stats  # Get extended statics from  elasticsearch to normalize data
    norm_value = Normalization.elasticsearch_forecast_normalize(
        es.elastic_get_forecast_data, sales_stats_data)  # Normalize the data using z-score technique
    norm_data = np.reshape(norm_value, (len(norm_value), 1))  # reshape the normalized value to pass it to model
    data = {"instances": [norm_data.tolist()]}
    sales_forecast_pred_url = Tensorflow_Base_URL + 'sales_forecast:predict'
    r = requests.post(url=sales_forecast_pred_url, data=json.dumps(data))  # Request to tensorflow RNN Model
    pred_val = json.loads(r.text)['predictions'][0]
    de_norm_value = DeNormalization.elasticsearch_forecast_denormalize(pred_val,
                                                                       sales_stats_data)  # Denormalize the data
    es_ingest_obj = Elasticsearch_ingest(de_norm_value)
    ingest_val = es_ingest_obj.forecast_ingest_fn(interval)
    es.ingest_es_forecast_data(ingest_val)  # Ingest into Elasticsearch
    ES_Logger.info_logs("Data Ingested into Elasticsearch")
    return "Data Ingested Successfully"


@bp.route('/rfm_ingest_pipeline')
def ingest_rfm_value():
    task = rfm_analysis.apply_async()
    return task.id
