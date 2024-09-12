FROM apache/airflow:2.10.0

COPY requirements.txt .env ./
COPY keys ./keys

RUN pip3.12 install --upgrade pip setuptools wheel \
    && pip3.12 install --no-cache-dir -r requirements.txt