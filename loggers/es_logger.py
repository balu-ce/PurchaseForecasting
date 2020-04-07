import logging


class ES_Logger:
    logging.basicConfig(filename='elasticsearch.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    @staticmethod
    def error_logs(message: str):
        logging.error(message)

    @staticmethod
    def warn_logs(message: str):
        logging.error(message)
