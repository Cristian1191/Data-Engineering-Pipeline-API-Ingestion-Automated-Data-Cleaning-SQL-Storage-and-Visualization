from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
import pandas as pd
import numpy as np
import datetime as dt

""" Crearemos métodos para la limpieza entorno a una clase, ya que nos permitirá manejar mejor el proceso sin tener que pasarlo a funciones"""

class DataCleaner:
    def __init__(self, df_input):
        self.df = df_input.copy()
        self.error_sumary = [] # Lista de errores
        # CORRECCION: Inicializamos las llaves aquí para no borrarlas luego por accidente
        self.report = {
            'data_quality_summary': {},
            'rows_deleted': {},
            'initial_row_count': len(self.df),
            'initial_col_count': len(self.df.columns)
        } 
        
    # 1. Método para correr el pipeline (en orden)
    def run_pipeline(self):
        print("--- Iniciando Pipeline ---")
        # a. Limpieza estructural
        self.rename_columns()
        self.remove_duplicates()
        
        # b. manejo de nulos y outliers
        self.handle_null_values()
        self.handle_outlier()
        
        # c. normalizamos y estandarizamos
        # Validamos tipos (esto puede crear nuevos nulos, ojo)
        self.validate_datatypes()
        
        # IMPORTANTE: Corremos nulos de nuevo para limpiar lo que rompió validate_datatypes
        self.handle_null_values()
        
        # self.validate_inter_columLogic() -> En futuro se podría aplicar un agente...
        self.validate_businessRules()
        
        # d. Tambien crearemos una funcion con sk-learn para los valores atípicos
        self.standardize_features() # Escalado Min/Max con scikit-learn
        
        # e. generamos el reporte
        self.generate_error_summary()
        self.generate_report()
        
        return self.df, self.report, self.error_sumary
    
    # Ahora empezamos con la lógica de los métodos
    def rename_columns(self):
        lista_nombreColumnas = self.df.columns.str.strip().str.replace('[^a-zA-Z0-9 ]', '', regex=True)
        lista_nueva_nombres_columna = [a.replace(" ", "_").lower() for a in lista_nombreColumnas]
        self.df.columns = lista_nueva_nombres_columna
        # Registramos
        self.report['columns_renamed'] = len(self.df.columns)
    
    # Eliminamos duplicados
    def remove_duplicates(self, subset=None): 
        n_df_original = len(self.df)
        self.df.drop_duplicates(keep='first', inplace=True)  # Mantenemos la primera ocurrencia
        # Ahora registramos lo ocurrido
        n_deleted = n_df_original - len(self.df) # Una simple diferencia nos dará las filas eliminadas
        self.report['rows_deleted']['duplicates'] = n_deleted

    # Ahora creamos el manejo de nulos y valores atípicos
    def handle_null_values(self):
        # Eliminaremos filas con más del 70% de NA
        n_total_rows = len(self.df)
        n_total_cols = len(self.df.columns)
        n_total_nulos_antes = self.df.isna().sum().sum()
        
        # Calculamos un umbral
        umbral = 0.30 * n_total_rows
        self.df.dropna(axis=1, thresh=umbral, inplace=True) # Axis=1 para buscar por columnas
        n_cols_dropped = n_total_cols - len(self.df.columns)
        
        for col in self.df.columns:
            if self.df[col].isnull().any():
                # si la columna es num
                if self.df[col].dtype in ['int64','float64']:
                    # Rellenamos con la media
                    mean_value = self.df[col].mean() # la media de toda la columna
                    self.df[col].fillna(mean_value, inplace=True)
                # si la columna es nominal/texto (objeto o string)
                elif self.df[col].dtype == 'object' or self.df[col].dtype.name =='category':
                    self.df[col].fillna('unknown', inplace=True)
        
        # Registramos
        n_total_nulos_despues = self.df.isna().sum().sum()
        
        acumulado_anterior = self.report['data_quality_summary'].get('nuls_imputed', 0)
        
        self.report['data_quality_summary']['columns_dropped_due_to_nuls'] = n_cols_dropped
        self.report['data_quality_summary']['nuls_imputed'] = acumulado_anterior + (n_total_nulos_antes - n_total_nulos_despues)
    
    # Ahora sigamos con los valores atípicos, para esto nos ayudaremos de scikit-learn
    def handle_outlier(self):
        # Dado que no sabemos como es el comportamiento de los datos no podemos usar un método que emplee una distribución normal
        # usaremos el método IQR (porcentiles)
        n_outlier_tratados = 0
        # Solo elegimos valores numéricos
        numeric_cols = self.df.select_dtypes(include=['int64','float64']).columns
        for col in numeric_cols:
            Q1 = self.df[col].quantile(0.25)
            Q3 = self.df[col].quantile(0.75)
            IQR = Q3 - Q1 
            # Caluclosmos los límites
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            # Identificamos los outliers
            outlier_count = len(self.df[(self.df[col] < lower_bound) | (self.df[col] > upper_bound)])
            n_outlier_tratados += outlier_count
            # Ahora reemplezamos los valores fuera de los límites 
            self.df[col] = self.df[col].clip(upper=upper_bound)
            self.df[col] = self.df[col].clip(lower=lower_bound)
        # registramos
        self.report['data_quality_summary']['outliers_treated'] = n_outlier_tratados

    # Ahora validamos los tipos de datos
    def validate_datatypes(self):
        n_total_nulos_antes = self.df.isna().sum().sum()
        
        # Trabajamos primero con columnas de tipo object
        for col in self.df.columns:
            if self.df[col].dtype == 'object':
                regex_pattern = r'[^0-9\.\-]*'
                temp = self.df[col].astype(str).str.replace(regex_pattern, '', regex=True)
                df_numeric = pd.to_numeric(temp, errors='coerce') # errores='coerce' convierne los fallos a NaN
                
                # Ahora viene un criterio importante -> Si menos del 1% de los datos limpios se convierten a NaN entonces 
                # estamos frente a una columna numérica y forzamos la conversión.
                umbral_de_numericos = 0.01
                if df_numeric.isnull().sum() < umbral_de_numericos * len(self.df):
                    # Entonces la columna es num
                    self.df[col] = df_numeric
                    continue
            
            if self.df[col].dtype == 'object':
                # Intenamos convertir a fechas ahora
                df_datatime = pd.to_datetime(self.df[col], errors='coerce')
                # Si al menos el 50% se convierten en fechas entonces tenemos una columna de fecha y forzamos .
                if df_datatime.notnull().sum() / len(self.df) > 0.5:
                    self.df[col] = df_datatime
        
        n_total_nulos_despues = self.df.isna().sum().sum()
        n_values_limips = n_total_nulos_despues - n_total_nulos_antes # Nota: aquí calculamos los nuevos nulos creados
        
        self.report['data_quality_summary']['values_coerced_to_null'] = n_values_limips

    def validate_businessRules(self):
        
        # Vamos a identificar si una columna es un ID
        # Con una fóumula sencilla. Razón de Unicidad = Num de únicos/Num?filas *100
        # Si R.U > umbral -> la columna es un ID 
        # Otro criterio -> Si existe una alta cardinalidad, pero no es un ID, entonces combinarlas generará igualmente demasiadas categorías y se aplicará otro criterio
        categorical_cols = self.df.select_dtypes(include=['object','category']).columns
        n_rows = len(self.df)
        # Existe una métrica directa para este caso. El coeficiente de variación
        # Umbrales
        UMBRAL_ID_UNICIDAD = 0.95 
        UMBRAL_MAX_CATEGORIAS = 100 # Límite absoluto para one-hot Encoder
        UMBRAL_FRECUENCIA_RARA = 0.005 # Categorias que aparecen menos del 0.5%
        
        n_new_unique_total = 0
        
        for col in categorical_cols:
            n_unique = self.df[col].nunique()
            unicity_radio = n_unique / n_rows
            if unicity_radio >= UMBRAL_ID_UNICIDAD:
                # Es un potencia ID
                continue
            elif n_unique > UMBRAL_MAX_CATEGORIAS:
                # Caso donde no es ID pero existe alta cardinalidad
                value_counts = self.df[col].value_counts(normalize=True)
                # Ahora identificamos las categorías raras
                rare_categories = value_counts[value_counts < UMBRAL_FRECUENCIA_RARA].index
                # Aplicamos la agrupacion
                if len(rare_categories) > 0:
                    self.df[col] = self.df[col].replace(rare_categories, 'Other')
                    # Sumamos al contador para el reporte
                    n_new_unique_total += len(rare_categories)
        
        self.report['data_quality_summary']['imputed_by_rules'] = n_new_unique_total
        
    def standardize_features(self):
        # Lo que haremos.
        # 1. Escala de variables numéricas usando StandarScaler
        # 2. Codificar variables categóricas usando OneHotEncoder
        # 3. Exluir columnas de Identificador y fechaas
        numeric_cols = self.df.select_dtypes(include=np.number).columns.tolist()
        categorical_cols = self.df.select_dtypes(include=['object','category']).columns.tolist()
        datetime_cols = self.df.select_dtypes(include=['datetime64']).columns.tolist()
        
        # Columnas como ID que no deben ser transformadas
        columns_to_exlude = []
        for col in numeric_cols:
            if self.df[col].nunique() / len(self.df) > 0.99:
                columns_to_exlude.append(col)
        
        # Seprar las columas a transformar
        numeric_features = [col for col in numeric_cols if col not in columns_to_exlude]
        categorical_features = categorical_cols
        
        numeric_transformer = StandardScaler()
        categorical_transformer = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
        
        # Construimos el columntransofrmer -> Aplica la transformación correcta a cada tipo de columna
        preprocesado = ColumnTransformer(transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ], remainder='passthrough')
        
        # Aplicamos la trans..
        transformed_data = preprocesado.fit_transform(self.df)
    
        # listado de columnas finales de categorias ohe y num
        ohe_feature_names = preprocesado.named_transformers_['cat'].get_feature_names_out(categorical_features)
        feature_names = numeric_features + ohe_feature_names.tolist()
        
        # Columnas sin transformar 
        all_untransformed_cols = columns_to_exlude + datetime_cols
        feature_names += all_untransformed_cols
        
        # Ajuste de seguridad por si las listas no cuadran exacto en longitud con el numpy array
        if len(feature_names) != transformed_data.shape[1]:
            # Recalculamos nombres genéricos si pasa algo raro
            feature_names = numeric_features + ohe_feature_names.tolist() + [f"col_{i}" for i in range(transformed_data.shape[1] - len(numeric_features) - len(ohe_feature_names))]
        self.df = pd.DataFrame(transformed_data, columns=feature_names, index=self.df.index)
        self.report['data_quality_summary']['final_features_count'] = len(self.df.columns)
        
    def generate_error_summary(self):
        if 'data_quality_summary' not in self.report:
            return
        
        summary = self.report['data_quality_summary']
        # 1. Extracción de Métricas de Limpieza
        n_total_nulos_imputados = summary.get('nuls_imputed', 0)
        # B. Valores Eliminados (Outliers y Lógica)
        n_outliers_deleted = self.report['rows_deleted'].get('duplicates', 0)
        # C. Transformación
        n_types_coerced = summary.get('values_coerced_to_null', 0)
        n_categories_grouped = summary.get('imputed_by_rules', 0)
        
        # 2. Creación del Resumen Estructurado Final
        final_summary = {
            "Filas_Originales": self.report.get('initial_row_count', 0),
            "Filas_Finales": len(self.df),
            "Filas_Eliminadas": n_outliers_deleted,
            "--- NULOS e IMPUTACIÓN ---": "",
            "Total_NaNs_Imputados": n_total_nulos_imputados,
            "NaNs_Creados_por_Conversión": n_types_coerced,
            "--- LIMPIEZA de CONTENIDO ---": "",
            "Outliers_Tratados": summary.get('outliers_treated', 0),
            "Categorías_Raras_Agrupadas": n_categories_grouped,
            "--- DIMENSIÓN FINAL ---": "",
            "Columnas_Originales": self.report.get('initial_col_count', 0),
            "Features_Finales_(OHE)": summary.get('final_features_count', 0)
        }
        self.report['error_summary'] = final_summary

    def generate_report(self):
        # Asegurarse de que el resumen haya sido generado
        self.generate_error_summary() 
        if 'error_summary' not in self.report:
            print("No se pudo generar el reporte.")
            return
        summary = self.report['error_summary']
        print("\n" + "="*50)
        print("REPORTE FINAL DEL PIPELINE")
        print("="*50)
        # Usaremos un formato de tabla simple
        for k, v in summary.items():
            print(f"{k:<30}: {v}")
        print("\n" + "="*50)