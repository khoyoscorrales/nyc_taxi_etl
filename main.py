# Orquestador principal del pipeline ETL de taxis NYC
from src.extract import download_parquet_from_url
from src.transform import transform_data
from src.utils.logging_config import setup_logging, PipelineMetrics
from src.refined import calculate_kpis
import os

logger = setup_logging()
metrics = PipelineMetrics()

def main():
    try:
        # Definición de rutas de archivos
        raw_path = 'data/raw/raw_2022_01.parquet'
        trusted_path = 'data/trusted/trusted_2022_01.parquet'
        zones_path = 'data/raw/taxi_zones.csv'
        
        # Capa Raw: Extracción de datos
        logger.info("Iniciando extracción de datos...")
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
        success = download_parquet_from_url(url, raw_path)
        if not success:
            logger.error("Fallo en la extracción de datos")
            return
        
        # Capa Trusted: Transformación y enriquecimiento
        logger.info("Iniciando transformación de datos...")
        success = transform_data(raw_path, trusted_path, zones_path)
        if not success:
            logger.error("Fallo en la transformación de datos")
            return

        # Capa Refined: KPIs y agregaciones
        logger.info("Calculando KPIs y agregaciones (Refined)...")
        refined_dir = 'data/refined'
        success = calculate_kpis(trusted_path, refined_dir)
        if not success:
            logger.error("Fallo en la generación de KPIs")
            return

        # Guardar métricas finales
        metrics.save_metrics()
        logger.info("Pipeline completado exitosamente")
    except Exception as e:
        logger.error(f"Error en el pipeline: {e}")

if __name__ == "__main__":
    main()
