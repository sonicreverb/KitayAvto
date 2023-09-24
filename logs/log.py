import logging
import os
BASE_DIR = os.path.dirname(os.path.dirname(__file__))


def setup_logging():
    logging.basicConfig(level=logging.INFO, filename=os.path.join(BASE_DIR, 'logs', 'logs.txt'), filemode='a',
                        format='%(asctime)s - %(levelname)s - %(message)s')


def log_info(message):
    logging.info(message)


def log_warning(message):
    logging.warning(message)


def log_error(message):
    logging.error(message)
