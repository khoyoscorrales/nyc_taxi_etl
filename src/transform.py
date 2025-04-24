# Módulo de transformación y enriquecimiento de datos (Capa Trusted)
import pandas as pd
from src.utils.logging_config import setup_logging
import os

logger = setup_logging()

def transform_data(input_path, output_path, zones_path):
    """
    Limpia, valida y enriquece los datos.
    Parámetros:
        input_path (str): Ruta del archivo Parquet crudo.
        output_path (str): Ruta donde guardar el archivo limpio.
        zones_path (str): Ruta del archivo CSV de zonas.
    Retorna:
        bool: True si la transformación fue exitosa, False en caso contrario.
    """
    try:
        # Cargar datos crudos y zonas
        logger.info("Cargando datos raw...")
        df = pd.read_parquet(input_path)
        zones_df = pd.read_csv(zones_path)
        
        # Limpieza y validación
        logger.info("Aplicando transformaciones...")
        df_cleaned = (df.pipe(validate_timestamps)
                       .pipe(validate_numeric_fields)
                       .pipe(remove_outliers))
        
        # Enriquecimiento con zonas geográficas
        logger.info("Enriqueciendo datos con zonas...")
        df_enriched = df_cleaned.merge(
            zones_df,
            left_on='PULocationID',
            right_on='LocationID',
            how='left'
        )
        
        # Guardar resultados
        logger.info(f"Guardando datos transformados en: {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_enriched.to_parquet(output_path)
        
        # Métricas de transformación
        logger.info(f"Registros procesados: {len(df)}")
        logger.info(f"Registros después de limpieza: {len(df_enriched)}")
        return True
    except Exception as e:
        logger.error(f"Error en transformación: {e}")
        return False

def validate_timestamps(df):
    """
    Filtra registros donde la fecha de recogida es menor que la de destino.
    """
    logger.info("Validando timestamps...")
    return df[df['tpep_pickup_datetime'] < df['tpep_dropoff_datetime']]

def validate_numeric_fields(df):
    """
    Filtra registros con distancia y tarifa mayores a cero.
    """
    logger.info("Validando campos numéricos...")
    return df[
        (df['trip_distance'] > 0) &
        (df['fare_amount'] > 0)
    ]

def remove_outliers(df):
    """
    Elimina outliers según reglas de negocio.
    """
    logger.info("Removiendo outliers...")
    return df[
        (df['trip_distance'] < 100) &  # Viajes menores a 100 millas
        (df['fare_amount'] < 1000)     # Tarifas menores a $1000
    ]