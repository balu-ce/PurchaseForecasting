# SalesForecasting
Forecast a sales data using LSTM Model

**Model Training**

1. Train a model a using LSTM layers by data in xls file provided.
2. Save the model in a folder model_predict

**Python Project**

1. Ingest all sales data to elasticsearch.(not in code but has to be done manually)
2. From python project query last 6 months sum of data.
3. Normalize the data using z-score method .
4. Call a model and predict a value for normalized data.
5. De-normalize the data using  reversing z-score method.
6. Ingest the data into elasticsearch with different index name.

