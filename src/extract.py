# Módulo de extracción de datos (Capa Raw)
import requests
import os
from src.utils.logging_config import setup_logging

logger = setup_logging()

def download_parquet_from_url(url, local_path):
    """
    Descarga un archivo Parquet desde una URL y lo guarda localmente.
    Parámetros:
        url (str): URL del archivo Parquet.
        local_path (str): Ruta local donde guardar el archivo.
    Retorna:
        bool: True si la descarga fue exitosa, False en caso contrario.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        logger.info(f"Iniciando descarga desde {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Archivo descargado exitosamente en: {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error al descargar archivo: {e}")
        return False