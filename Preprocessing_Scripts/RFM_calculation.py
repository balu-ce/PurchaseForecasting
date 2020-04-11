import json

import pandas as pd
from datetime import datetime
from sklearn.cluster import KMeans

class RFM_Analysis:
    def __init__(self, data):
        self.df = pd.DataFrame.from_dict(data)

    @staticmethod
    def order_cluster(cluster_field_name, target_field_name, data_frame, ascending):
        df_new = data_frame.groupby(cluster_field_name)[target_field_name].mean().reset_index()
        df_new = df_new.sort_values(by=target_field_name, ascending=ascending).reset_index(drop=True)
        df_new['index'] = df_new.index
        df_final = pd.merge(data_frame, df_new[[cluster_field_name, 'index']], on=cluster_field_name)
        df_final = df_final.drop([cluster_field_name], axis=1)
        df_final = df_final.rename(columns={"index": cluster_field_name})
        return df_final

    def rfm_calc(self):
        df = self.df
        df['Order_Date'] = pd.to_datetime(df['Order_Date'])
        tx_1y = df[(df.Order_Date <= datetime(2017, 12, 31)) & (df.Order_Date >= datetime(2017, 1, 1))].reset_index(
            drop=True)
        tx_1y['Sales'] = df['Sales'].astype(float)
        tx_user = pd.DataFrame(tx_1y['Customer_ID'].unique())

        tx_user.columns = ['Customer_ID']
        # FEATURE ENGINEERING
        tx_max_purchase = tx_1y.groupby('Customer_ID').Order_Date.max().reset_index()
        tx_max_purchase.columns = ['Customer_ID', 'MaxPurchaseDate']

        tx_max_purchase['Recency'] = (
                tx_max_purchase['MaxPurchaseDate'].max() - tx_max_purchase['MaxPurchaseDate']).dt.days
        tx_user = pd.merge(tx_user, tx_max_purchase[['Customer_ID', 'Recency']], on='Customer_ID')

        kmeans = KMeans(n_clusters=4)
        kmeans.fit(tx_user[['Recency']])
        tx_user['RecencyCluster'] = kmeans.predict(tx_user[['Recency']])

        tx_user = self.order_cluster('RecencyCluster', 'Recency', tx_user, True)

        # get total purchases for frequency scores
        tx_frequency = tx_1y.groupby('Customer_ID').Order_Date.count().reset_index()
        tx_frequency.columns = ['Customer_ID', 'Frequency']
        tx_user = pd.merge(tx_user, tx_frequency[['Customer_ID', 'Frequency']], on='Customer_ID')

        # clustering for frequency
        kmeans = KMeans(n_clusters=4)
        kmeans.fit(tx_user[['Frequency']])
        tx_user['FrequencyCluster'] = kmeans.predict(tx_user[['Frequency']])

        # order frequency clusters
        tx_user = self.order_cluster('FrequencyCluster', 'Frequency', tx_user, True)

        # calculate monetary value, create a dataframe with it
        tx_revenue = tx_1y.groupby('Customer_ID').Sales.sum().reset_index()
        tx_revenue.columns = ['Customer_ID', 'Sales']
        tx_user = pd.merge(tx_user, tx_revenue, on='Customer_ID')

        # Revenue clusters
        kmeans = KMeans(n_clusters=4)
        kmeans.fit(tx_user[['Sales']])
        tx_user['RevenueCluster'] = kmeans.predict(tx_user[['Sales']])

        # ordering clusters and who the characteristics
        tx_user = self.order_cluster('RevenueCluster', 'Sales', tx_user, True)
        tx_user.groupby('RevenueCluster')['Sales'].describe()

        # create a dataframe with CustomerID and Invoice Date
        tx_day_order = tx_1y[['Customer_ID', 'Order_Date']]

        # convert Invoice Datetime to date
        tx_day_order['Order_Date'] = tx_day_order['Order_Date'].dt.date

        tx_day_order = tx_day_order.sort_values(['Customer_ID', 'Order_Date'])  # drop duplicates
        tx_day_order = tx_day_order.drop_duplicates(subset=['Customer_ID', 'Order_Date'], keep='first')

        # shifting last 3 purchase dates
        tx_day_order['PrevInvoiceDate'] = tx_day_order.groupby('Customer_ID')['Order_Date'].shift(1)
        tx_day_order['T2InvoiceDate'] = tx_day_order.groupby('Customer_ID')['Order_Date'].shift(2)
        tx_day_order['T3InvoiceDate'] = tx_day_order.groupby('Customer_ID')['Order_Date'].shift(3)

        tx_day_order['DayDiff'] = (tx_day_order['Order_Date'] - tx_day_order['PrevInvoiceDate']).dt.days
        tx_day_order['DayDiff2'] = (tx_day_order['Order_Date'] - tx_day_order['T2InvoiceDate']).dt.days
        tx_day_order['DayDiff3'] = (tx_day_order['Order_Date'] - tx_day_order['T3InvoiceDate']).dt.days

        # Calculate aggs for std_dev and mean for day difference
        tx_day_diff = tx_day_order.groupby('Customer_ID').agg({'DayDiff': ['mean', 'std']}).reset_index()
        tx_day_diff.columns = ['Customer_ID', 'DayDiffMean', 'DayDiffStd']

        tx_day_order_last = tx_day_order.drop_duplicates(subset=['Customer_ID'], keep='last')
        tx_day_order_last = tx_day_order_last.dropna()
        tx_day_order_last = pd.merge(tx_day_order_last, tx_day_diff, on='Customer_ID')

        tx_user = pd.merge(tx_user, tx_day_order_last[
            ['Customer_ID', 'DayDiff', 'DayDiff2', 'DayDiff3', 'DayDiffMean', 'DayDiffStd']], on='Customer_ID')
        return json.loads(tx_user.reset_index().to_json(orient='records'))
