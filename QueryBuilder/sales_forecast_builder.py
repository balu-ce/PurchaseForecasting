class Sales_Query_Builder:

    @staticmethod
    def get_avg_query():
        query = {
            "size": 0,
            "query": {
                "range": {
                    "Order_Date": {
                        "lte": "2015-12-31"
                    }
                }
            },
            "aggs": {
                "mean_of_sales": {
                    "date_histogram": {
                        "field": "Order_Date",
                        "calendar_interval": "month"
                    },
                    "aggs": {
                        "sales": {
                            "sum": {
                                "field": "Sales"
                            }
                        },
                        "sales_bucket_sort": {
                            "bucket_sort": {
                                "sort": [
                                    {"_key": {"order": "desc"}}
                                ]
                            }
                        }
                    }
                },
                "stats_sales": {
                    "extended_stats_bucket": {
                        "buckets_path": "mean_of_sales>sales"
                    }
                }
            }
        }
        return query

    @staticmethod
    def get_forecast_query():
        query = {
            "size": 0,
            "aggs": {
                "sales_per_month": {
                    "date_histogram": {
                        "field": "Order_Date",
                        "calendar_interval": "month"
                    },
                    "aggs": {
                        "sales_sum": {
                            "sum": {
                                "field": "Sales"
                            }
                        },
                        "sales_bucket_sort": {
                            "bucket_sort": {
                                "sort": [
                                    {"_key": {"order": "desc"}}
                                ],
                                "size": 7
                            }
                        }
                    }
                }
            }
        }
        return query

    @staticmethod
    def get_all_sales_query():
        query = {
            "query": {
                "match_all": {}
            }
        }
        return query
