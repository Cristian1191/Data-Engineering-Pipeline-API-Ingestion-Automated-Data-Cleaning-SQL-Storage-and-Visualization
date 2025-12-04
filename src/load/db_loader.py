from sqlalchemy import create_engine,String, Integer
from sqlalchemy.orm import DeclarativeBase, mapped_column
import pandas as pd

connection_url = 'mysql://root:cris123@localhost:3306/etl_db'
engine = create_engine(connection_url)

# Ahora creamos la clase para luego instanciar nuestra BD
class User(Base):
    __tablename__ = 'root'
    id = mapped_column(primary_key=True)
    name = mapped_column(String(30))
    