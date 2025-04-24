# Módulo de agregación y KPIs (Capa Refined)
import pandas as pd
import os
import json
from src.utils.logging_config import setup_logging

logger = setup_logging()

def calculate_kpis(trusted_path, output_dir):
    """
    Calcula KPIs y agregaciones a partir de los datos limpios.
    Parámetros:
        trusted_path (str): Ruta del archivo Parquet limpio.
        output_dir (str): Carpeta donde guardar los resultados.
    Retorna:
        bool: True si el cálculo fue exitoso, False en caso contrario.
    """
    try:
        df = pd.read_parquet(trusted_path)
        os.makedirs(output_dir, exist_ok=True)
        kpis = {}

        # A. Patrón de Demanda y Tiempos Pico
        df['pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['hour'] = df['pickup_datetime'].dt.hour
        df['weekday'] = df['pickup_datetime'].dt.day_name()
        demand = df.groupby(['weekday', 'hour']).size().reset_index(name='num_trips')
        demand.to_csv(os.path.join(output_dir, 'demand_by_hour_weekday.csv'), index=False)
        top_demand = demand.sort_values('num_trips', ascending=False).head(10)
        kpis['demand_by_hour_weekday_top'] = top_demand.to_dict(orient='records')

        # B. Eficiencia Geográfica y Económica
        df['trip_time'] = (pd.to_datetime(df['tpep_dropoff_datetime']) - df['pickup_datetime']).dt.total_seconds() / 60
        zone_stats = df.groupby('Zone').agg(
            avg_income_per_mile=('fare_amount', lambda x: x.sum() / df.loc[x.index, 'trip_distance'].sum() if df.loc[x.index, 'trip_distance'].sum() > 0 else 0),
            avg_speed_mph=('trip_distance', lambda x: x.sum() / (df.loc[x.index, 'trip_time'].sum() / 60) if df.loc[x.index, 'trip_time'].sum() > 0 else 0),
            avg_fare=('fare_amount', 'mean'),
            num_trips=('fare_amount', 'count')
        ).reset_index()
        zone_stats.to_csv(os.path.join(output_dir, 'zone_stats.csv'), index=False)
        kpis['top_zones_by_income_per_mile'] = zone_stats.sort_values('avg_income_per_mile', ascending=False).head(5).to_dict()

        # C. Impacto de la Calidad de Datos
        kpis['total_records_after_cleaning'] = len(df)

        # Guardar KPIs en JSON
        with open(os.path.join(output_dir, 'kpis.json'), 'w') as f:
            json.dump(kpis, f, indent=4)
        logger.info("KPIs calculados y guardados en capa Refined.")
        return True
    except Exception as e:
        logger.error(f"Error calculando KPIs: {e}")
        return False