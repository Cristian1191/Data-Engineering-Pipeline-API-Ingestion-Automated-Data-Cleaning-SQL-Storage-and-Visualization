Data Engineering Pipeline - Ingestion, Cleaning, Storage and Visualization

Descripcion del Proyecto

Este proyecto implementa un pipeline automatizado de ingestion, limpieza, transformacion y almacenamiento de datos. El sistema toma uno o varios tipos de archivos de datos (CSV, Excel) y ejecuta un proceso completo de validacion, limpieza y normalizacion, almacenando los resultados en una base de datos SQL y generando reportes visuales.

¿Que hace el Pipeline?

1. Recibe archivos de datos en diferentes formatos (CSV, Excel)
2. Extrae los datos a través de una API de cliente configurable
3. Aplica reglas de limpieza y validacion automatizadas
4. Transforma y normaliza los datos segun las reglas establecidas
5. Carga los datos limpios en una base de datos SQL
6. Genera reportes y metricas de calidad
7. Visualiza los resultados en un dashboard interactivo

Caso Practico

Si tienes un reporte de ventas semanal, un listado de alumnos, inventario u otro conjunto de datos, normalmente tendrias que:
- Eliminar valores faltantes (NA)
- Arreglar errores de formato
- Validar columnas y tipos de datos
- Normalizar valores
- Verificar integridad de datos
- Generar reportes manuales

Este proyecto automatiza TODO esto con un unico comando.

Funcionalidades Principales

Modulo de Extraccion (Extract):
- Lectura de archivos CSV y Excel
- Conexion con APIs externas
- Validacion inicial de formato
- Manejo de errores en la lectura

Modulo de Transformacion (Transform):
- Limpieza de datos y valores nulos
- Normalizacion de formatos
- Validacion de reglas de negocio
- Deteccion y tratamiento de valores anomalos
- Formateo de fechas y numeros

Modulo de Carga (Load):
- Conexion a base de datos SQL
- Insercion de datos limpios
- Manejo de transacciones
- Logging de operaciones

Generacion de Reportes:
- Resumen de errores encontrados
- Metricas de calidad de datos
- Reporte de validaciones realizadas
- Estadisticas de transformacion

Dashboard de Visualizacion:
- Graficos de calidad de datos
- Distribucion de valores
- Tendencias y anomalias
- Historial de procesamiento

Requisitos

Python 3.12.x (version estable recomendada)

Instalacion

1. Clonar el repositorio

2. Instalar las dependencias necesarias

pip install -r requirements.txt

3. Configurar las variables de entorno necesarias en config.py

4. Ejecutar el pipeline principal

python main.py

Estructura del Proyecto

- src/: Codigo fuente principal
  - extract/: Modulo de extraccion de datos
  - transform/: Modulo de limpieza y transformacion
  - load/: Modulo de carga a base de datos
  - utils/: Funciones utilitarias y configuracion
- dashboard/: Interfaz de visualizacion
- data/: Archivos de datos y almacenamiento
- logs/: Archivos de registro y auditoría
- main.py: Script principal del pipeline
- requirements.txt: Dependencias del proyecto

Uso

Para ejecutar el pipeline completo:

python main.py --input archivo.csv --output datos_limpios.csv

El sistema generara automaticamente:
- Archivo de datos limpios
- Reporte de calidad
- Resumen de errores y advertencias
- Registros de auditoria
