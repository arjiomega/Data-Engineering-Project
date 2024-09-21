FROM apache/airflow:2.10.0

# Directories
COPY ../keys ./keys

# Individual Files 
COPY ../requirements.txt \
    ../.env \
    ../project_dbt/dbt_project.yml \
    ../project_dbt/packages.yml \
    ../project_dbt/profiles.yml \
    ./

RUN pip3.12 install --upgrade pip setuptools wheel \
    && pip3.12 install --no-cache-dir -r requirements.txt

RUN dbt deps