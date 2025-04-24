# NYC Taxi ETL Pipeline

## Descripción

Este proyecto implementa un pipeline ETL para procesar datos públicos de taxis amarillos de NYC, siguiendo el patrón de arquitectura de medallón (Raw, Trusted, Refined). El pipeline descarga, limpia, enriquece y calcula KPIs de negocio sobre los datos.

---

## Estructura del pipeline

- **Raw:** Descarga los datos originales desde la web.
- **Trusted:** Limpia y enriquece los datos, eliminando registros erróneos y uniendo información geográfica.
- **Refined:** Calcula KPIs y agregaciones para análisis de negocio.

---

## Instalación y uso del entorno virtual

1. Clona el repositorio:
   ```bash
   git clone https://github.com/khoyoscorrales/nyc_taxi-etl.git
   cd nyc_taxi-etl
   ```

2. Crea y activa el entorno virtual (Windows):
   ```bash
   python -m venv env
   .\env\Scripts\activate
   ```
   En Mac/Linux:
   ```bash
   python3 -m venv env
   source env/bin/activate
   ```

3. Instala las dependencias:
   ```bash
   pip install -r requirements.txt
   ```

4. Descarga el archivo de zonas y colócalo en `data/raw/taxi_zones.csv`:
   [Taxi Zone Lookup Table (CSV)](https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv)

5. Ejecuta el pipeline:
   ```bash
   python main.py
   ```

6. (Opcional) Desactiva el entorno virtual:
   ```bash
   deactivate
   ```

---

## Resultados

- Datos limpios: `data/trusted/trusted_2022_01.parquet`
- KPIs y agregaciones: `data/refined/`
- Logs y métricas: `logs/`

---

## KPIs calculados

- **Demanda por hora y día:** Número de viajes por franja horaria y día de la semana.
- **Eficiencia por zona:** Ingreso promedio por milla, velocidad promedio, ranking de zonas más rentables.
- **Impacto de la limpieza:** Porcentaje de registros descartados.

---

## Observabilidad

- El pipeline registra cada paso y error en archivos de log.
- Se generan métricas de ejecución y calidad de datos.

---

## Estructura del código

```
data/
    raw/
    refined/
    trusted/
logs/
src/
  extract.py
  transform.py
  refined.py
  utils/
    logging_config.py
main.py
requirements.txt
```

---

## Decisiones técnicas

- **Pandas:** Se eligió pandas por su facilidad de uso y eficiencia para procesar archivos individuales en local.
- **Arquitectura de medallón:** Permite separar claramente las etapas de procesamiento y facilita la trazabilidad y el mantenimiento.
- **Logging y manejo de errores:** Cada etapa está protegida con try/except y se registran logs detallados para facilitar el monitoreo y la depuración.
- **Extensibilidad:** El pipeline está modularizado, lo que permite agregar nuevas reglas de limpieza o KPIs fácilmente.

---

## Ejemplo de ejecución y resultados

Al ejecutar el pipeline, verás logs como estos:
```
Iniciando extracción de datos...
Archivo descargado exitosamente en: data/raw/raw_2022_01.parquet
Iniciando transformación de datos...
Registros procesados: 2463931
Registros después de limpieza: 2421241
Calculando KPIs y agregaciones (Refined)...
KPIs calculados y guardados en capa Refined.
Pipeline completado exitosamente
```

---

## Cómo cambiar el mes o año de los datos

Solo debes modificar la URL en `main.py` para descargar otro archivo de la fuente oficial.

---

## Contacto

Desarrollado por Kevin Hoyos Corrales.  
Contacto: kevin-h12@hotmail.com
