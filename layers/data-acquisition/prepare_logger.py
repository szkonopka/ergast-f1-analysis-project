import logging

def get_configured(logger_name, file_handler_name):
    logger = logging.getLogger('ergast_handle_api')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('ergast_handle_api.log')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger