from celery import Celery
def make_celery(app_name=__name__):
    redis_uri = 'redis://127.0.0.1:6379/0'
    return Celery(app_name, backend=redis_uri, broker=redis_uri)
celery = make_celery()
