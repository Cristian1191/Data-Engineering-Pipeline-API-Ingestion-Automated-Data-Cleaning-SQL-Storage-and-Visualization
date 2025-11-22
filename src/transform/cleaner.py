# Limpieza y normalización de datos
import pandas as pd
""" Crearemos métodos para la limpieza entorno a una clase, ya que nos permitirá manejar mejor el proceso sin tener que pasarlo a funciones"""
class DataCleaner:
    def __init__(self,df_input):
        self.df= df_input.copy()
        self.error_sumary = [] # Lista de errores
        self.report= {} # diccionario para el reporte final 
    # 1. Método para correr el pipeline (en orden)
    def run_pipeline(self):
        # a. Limpieza estructural
        self.rename_columns()
        self.remove_duplicates()
        # b. manejo de nulos y outliers
        self.handle_null_values()
        self.handle_outlier()
        # c. normalizamos y estandarizamos
        self.validate_datatypes()
        self.validate_inter_columLogic()
        self.validate_businessRules()
        # d. Tambien crearemos una funcion con sk-learn para los valores atípicos
        self.standardize_features() # Escalado Min/Max con scikit-learn
        # e. generamos el reporte
        self.generate_error_sumary()
        self.generate_report()
        return self.df, self.report, self.error_sumary
    # Ahora empezamos con la lógica de los métodos
    def rename_columns(self):
        lista_nombreColumnas= self.df.columns.str.strip().str.replace('[^a-zA-Z0-9 ]', '', regex=True)
        lista_nueva_nombres_columna = [ a.replace(" ","_").lower() for a in lista_nombreColumnas]
        self.df.columns = lista_nueva_nombres_columna
        # Registramos
        self.report['columns_renamed'] = len(self.df.columns)