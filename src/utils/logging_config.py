# Configuración de logging y métricas para el pipeline
import logging
import os
import json
from datetime import datetime

def setup_logging():
    """
    Configura el sistema de logging para el pipeline.
    """
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class PipelineMetrics:
    """
    Clase para registrar métricas del pipeline.
    """
    def __init__(self):
        self.start_time = datetime.now()
        self.metrics = {
            'extract': {'records': 0, 'time': 0},
            'transform': {'records': 0, 'filtered': 0, 'time': 0},
            'load': {'records': 0, 'time': 0},
            'kpis': {}
        }
    def update_stage_metrics(self, stage, records, filtered=0):
        self.metrics[stage]['records'] = records
        if filtered:
            self.metrics[stage]['filtered'] = filtered
        self.metrics[stage]['time'] = (datetime.now() - self.start_time).total_seconds()
    def save_metrics(self):
        with open(f'logs/metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
            json.dump(self.metrics, f, indent=4)