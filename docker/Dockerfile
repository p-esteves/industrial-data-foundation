FROM apache/airflow:2.10.2

USER root
# Instalar dependências de sistema se necessário
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copiar requirements e instalar
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
