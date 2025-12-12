import logging

def setup_logger(name:str = "etl")->logging.Logger:

    """
        Configure and return a logger instance for ETL
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler =logging.StreamHandler() #for Airflow to read the LOGS
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
    return logger