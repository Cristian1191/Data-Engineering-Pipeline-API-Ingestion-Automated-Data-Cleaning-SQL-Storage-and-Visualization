# Variables del entorno
import logging
import os
from .config import settings
def setup_logger(name=__name__):
    # Crear carpeta de logs si no existe
    if not os.path.exists(settings.LOGS_DIR):
        os.makedirs(settings.LOGS_DIR)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Formato del log
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Handler para archivo
    file_handler = logging.FileHandler(os.path.join(settings.LOGS_DIR, "etl.log"))
    file_handler.setFormatter(formatter)

    # Handler para consola
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger