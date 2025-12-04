import os
from dotenv import load_dotenv
load_dotenv() # Cargamos las variables del archivo .env
class Config:
    PROJECT_NAME= 'ETL Data Cleaner API'
    VERSION = '0.1'
    DB_NAME = os.getenv("DB_NAME", "etl_db")
    DB_URL = f"mysql:///{DB_NAME}"
    # Directorios
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    LOGS_DIR = os.path.join(BASE_DIR, "logs")
    
settings= Config()