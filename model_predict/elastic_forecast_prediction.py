import json

import tensorflow as tf
import numpy as np
import sys

sys.path.append("..")

from Preprocessing_Scripts.normalization import normalization
from Preprocessing_Scripts.denormalization import deNormalization

from db_operations.elasticsearch_oper import Elastic_connect

model = tf.keras.models.load_model(
    "../models/sales_forecast/1/"
)

data = Elastic_connect("sales_data")

with open('../query_objects/forcast_data.json') as f:
    forecast_query = json.load(f)

with open('../query_objects/mean_variance.json') as f:
    stats_data_query = json.load(f)

forecast_data = data.elastic_get_data(forecast_query)
stats_data = data.elastic_get_data(stats_data_query)

print(forecast_data)

norm_obj = normalization(forecast_data)

print(norm_obj)

norm_data = norm_obj.elasticsearch_forecast_normalize(stats_data)

norm_data = [np.reshape(norm_data, (len(norm_data), 1))]

norm_data = tf.convert_to_tensor(norm_data, dtype=tf.float64)

print(norm_data)

temp = model.predict(norm_data)[0]

print(temp)

denorm = deNormalization(temp)

final_data = denorm.elasticsearch_forecast_denormalize(stats_data)

for x in final_data:
    print(x)
