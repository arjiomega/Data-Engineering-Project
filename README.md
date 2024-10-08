# Data-Engineering-Project

![pipeline](https://raw.githubusercontent.com/arjiomega/Data-Engineering-Project/refs/heads/main/resources/pipeline.png)

## Table of Contents
1. [Project Overview](#project-overview)

    1. [Data Sources](#data-sources)
    2. [ELT/ETL Process](#elt-process)
    3. [Technologies](#technologies)
    4. [Use Cases](#use-cases)
    5. [Challenges](#challenges)

2. [Setup](#setup)

    1. [Setup Airflow](#setup-airflow)
    2. [Setup GCP Account](#setup-gcp-account)

## Project Overview

The primary goal of this project is to develop an end-to-end data engineering pipeline using NYC TLC data. The pipeline includes:

    Data Extraction: Extracting data and storing it in Google Cloud Storage (GCS).
    Data Loading: Loading raw data from GCS into Google BigQuery.
    Data Transformation: Preparing staging tables in BigQuery and creating a star schema.
    Visualization: Using Google Looker Studio for data visualization.
    Future Enhancements: Implementing machine learning models in BigQuery.

This project aims to provide a comprehensive data pipeline that spans the entire data engineering workflow, offering insights into the NYC taxi data and enabling further data analysis and machine learning opportunities.

### Data Sources

The data source for this project is the NYC TLC (Taxi and Limousine Commission) data, which includes records of taxi trips in New York City. The dataset typically consists of:

    Trip Records: Details of individual taxi trips, including pickup and dropoff locations, fare amounts, and more.
    Frequency of Updates: The data is regularly updated, reflecting ongoing taxi trip records.

### Data Warehouse Design

**Data Warehouse Star Schema**
![nyc_tlc_trip_star_schema](https://raw.githubusercontent.com/arjiomega/Data-Engineering-Project/refs/heads/main/resources/nyc_tlc_trip_star_schema.png)

### ELT Process

The ELT (Extract, Load, Transform) process for this project is as follows:

    Extraction: Data is extracted from the NYC TLC dataset and uploaded to Google Cloud Storage.
    Loading: The raw data is loaded into Google BigQuery.
    Transformation:
        Staging: Data is organized into staging tables within BigQuery.
        Star Schema: A star schema is created to facilitate efficient querying and analysis.

The pipeline will be set up using tools like Airflow for orchestration, dbt for transformations, and Google Looker Studio for visualization. Future enhancements will include the implementation of machine learning models in BigQuery.

### Technologies

This project utilizes the following technologies and tools:

    Airflow: For orchestrating the ETL/ELT pipeline.
    dbt: For data transformations and modeling.
    Google BigQuery: For data storage and analysis.
    Google Cloud Storage (GCS): For storing raw data.
    Google Looker Studio: For data visualization and reporting.
    Docker: For containerizing the development environment.
    Terraform: For infrastructure as code and provisioning cloud resources.

### Use Cases

While specific use cases are still under exploration, potential benefits of the data pipeline include:

    Data Analysis: Analyzing NYC taxi trip data for trends, patterns, and insights.
    Reporting: Creating reports and visualizations for stakeholders using Google Looker Studio.
    Machine Learning: Implementing predictive models to forecast taxi demand, optimize routes, or analyze passenger behavior.

## Setup
### Setup Airflow

#### 1a. Build Docker Image
Build the Docker image for Airflow to ensure it has access to keys, .env, and other necessary files:
```bash
docker build -f airflow/Dockerfile . --tag extending_airflow:latest
```


#### 1b. Prepare Environment Variables
Create the .env file with the following content:
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

complete `.env` file with:
```.env
AIRFLOW_UID=1000
CAB_DATA_BASE_URL='https://d37ci6vzurychx.cloudfront.net/trip-data'

GOOGLE_APPLICATION_CREDENTIALS='./keys/gcp-creds.json'
DBT_GOOGLE_PROJECT_DEV=<project_name>
DBT_GOOGLE_BIGQUERY_DATASET_DEV='nyc_taxi_data'
DBT_PROFILES_DIR='./'
```
- make sure gcp-creds.json is in keys directory
- `CAB_DATA_BASE_URL` may change in the future. if that happens, update it from [nyc tlc trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

#### 1c. Initialize Database and Create User Account
Run the following command to initialize the database and set up the user account:
```bash
docker compose up airflow-init
```


#### 1d. Start Airflow
Start the Airflow services with the following command:
```bash
docker compose --env-file .env -f airflow/docker-compose.yaml up --build -d
```
- access airflow web ui `localhost:8080`
- User and Password: `airflow`

#### Optional: Stop Airflow
If needed, you can stop the Airflow services with:
```bash
docker compose --env-file .env -f airflow/docker-compose.yaml down
```

### Setup GCP Account

1. Create service account and add the following roles
bigquery admin
cloud storage admin

2. download key in json format and save to keys directory as gcp-creds.json



