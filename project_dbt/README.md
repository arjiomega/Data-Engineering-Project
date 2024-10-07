# Project DBT

This repository contains the dbt project for managing the ETL/ELT pipeline. The project consists of two main sections: staging and core. The structure and commands are organized for easy development, testing, and production deployment.

## Project Structure

The directories and files in this project follow a specific naming convention to avoid conflicts during Docker Compose runs, especially when interfacing with Airflow. All dbt-related directories have been prefixed with dbt_.
Main Sections:

### Main Sections:

**dbt_models/staging**:
    This section contains the staging models, which are a one-to-one representation of the raw data. They include stg_bigquery__green_cab.sql, stg_bigquery__yellow_cab.sql, and their associated .yml files.
    
**dbt_models/core**:
    This section represents the data warehouse and contains transformed models, including dimensions (dim_*) and the fact_trips table.

### Other Directories:

- **dbt_analyses**/: Contains analysis queries used for advanced insights and reporting.
- **dbt_macros**/: Houses custom macros for null handling and other logic (null_zero_values.sql, null_negative_values.sql, etc.).
- **dbt_seeds**/: Stores seed files such as taxi_zone_lookup.csv used for lookups or reference data.
- **dbt_snapshots**/: Manages snapshots for tracking data changes over time.
- **dbt_tests**/: Contains custom tests to ensure data quality and integrity.
- **logs**/: Stores logs, including dbt.log for tracking dbt runs.
- **dbt_project.yml**: Configuration file for the dbt project, including model paths and other settings.
- **packages.yml**: Lists external dbt packages used in the project.
- **profiles.yml**: Contains connection information for the data warehouse.

## Running the Models

You can run and test specific sections of the project using the following commands:
### Run Models:

Run staging models:
```bash
dbt run --select staging
```

Run core models (data warehouse):

```bash
dbt run --select core
```

### Run Tests:

Test staging models:
```bash
dbt test --select staging
```

Test core models (data warehouse):
```bash
dbt test --select core
```

## Notes

    Make sure that profiles.yml is correctly configured for your environment before running any commands.
    The dbt_ directory naming convention was implemented to avoid conflicts with Airflow during Docker Compose.