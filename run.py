from app import factory
import app
if __name__ == "__main__":
    app = factory.create_app("sales_analysis", celery=app.celery)
    app.run(debug=True)
