# Analysis of Chicago 311 Service Requests

The City of Chicago regularly collects and publishes many types of city-related data. Among them are 311 city service requests: https://data.cityofchicago.org/Service-Requests/311-Service-Requests/v6vf-nfxy/about_data

In this project, I developed an automated end-to-end ELT data pipeline that extracts the past two years of service requests data via their API; store the data to Google Cloud Storage; load it to BigQuery for transformation with Python (Pandas); and then import the processed data to Power BI for visual analysis.

Complementary data tables that were created manually are (1). Data on Chicago's sides (e.g., North Side), community areas (e.g., Humboldt Park), and their respective 2022 populations; and (2). Table of categories, subcategories, and types that service requests can fall under.

My interactive Power BI dashboard can be viewed here (data collected on Mar. 17, 2025):

The pbix file can be downloaded here:

## ELT Pipeline and Data Model

- **csr_dag.py:** This Python script creates the Airflow DAG, pictured below:
  - <image>
  - The executed tasks are
    - **latest_only:** My DAG is scheduled to run every week, but if there any missed runs, this task ensures that the DAG is executed only once to get the most recent two years of data.
    - **download_data:** This task downloads service requests data from the API provided by the City of Chicago.
    - **upload_to_gcs:** 
