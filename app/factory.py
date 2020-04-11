from flask import Flask

from app.celery_utils import init_celery


def create_app(app_name, **kwargs):
    app = Flask(app_name)
    if kwargs.get("celery"):
        init_celery(kwargs.get("celery"), app)
    from app.routes import bp
    app.register_blueprint(bp)
    print("Reached")
    return app
