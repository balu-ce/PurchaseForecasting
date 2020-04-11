from app import celery
from app.factory import create_app
from app.celery_utils import init_celery
import app

app = create_app('sales_analysis', celery=app.celery)
init_celery(celery, app)
