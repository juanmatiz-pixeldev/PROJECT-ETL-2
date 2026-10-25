# Imagen del pipeline: Airflow 3.1.8 + dependencias del proyecto (GX, pandas, etc.)
FROM apache/airflow:3.1.8

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# El codigo se monta como volumen en docker-compose (desarrollo); PYTHONPATH expone el paquete `etl`
ENV PYTHONPATH=/opt/airflow/src
