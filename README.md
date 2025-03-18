# Analysis of Chicago 311 Service Requests

The City of Chicago regularly collects and publishes many types of city-related data. Among them are 311 city service requests (SRs): https://data.cityofchicago.org/Service-Requests/311-Service-Requests/v6vf-nfxy/about_data

In this project, I developed an automated end-to-end ELT data pipeline that extracts the past two years of SR data via their API; store the data to Google Cloud Storage; load it to BigQuery for transformation with Python (Pandas); and then import the processed data to Power BI for visual analysis.

Complementary data tables that I created manually are (1). Table of Chicago's sides (e.g., North Side) and community areas (e.g., Humboldt Park); and (2). Table of categories, subcategories, and types that service requests can fall under, which I obtained directly from the city's SR portal.

My interactive Power BI dashboard can be viewed here (data collected on Mar. 17, 2025):

The pbix file can be downloaded here:

## ELT Pipeline and Data Model

- **csr_dag.py:** This Python script creates the Airflow DAG, pictured below:
  - <image>
  - The executed tasks are
    - **latest_only:** My DAG is scheduled to run every week, but if there any missed runs, this task ensures that the DAG is executed only once to get the most recent two years of data.
    - **download_data:** This task downloads SR data from the API provided by the City of Chicago to the Airflow Docker container.
    - **upload_to_gcs:** Uploads the SR data from the container to Google Cloud Storage bucket.
    - **load_gcs_to_bq_csr:** Loads the raw SR data from GCS bucket to BigQuery.
    - **load_bcs_to_bq_ca:** I manually created data on the city's sides/community areas and uploaded it to my GCS bucket. This task loads this data table to BigQuery.
    - **load_gcs_to_bq_categories:** I manually created data on available SR categories, subcategories, and types. This task loads this data table to BigQuery.
    - **run_bq_transform_sr_data:** This task runs the **transform_sr_data** function from the **csr_bq_transform_data.py** script (more on this script later) to process the raw SRs and categories dataset for visualization.
    - **run_bq_transform_community_areas_data:** Runs the **community_areas** function from **csr_bq_transform_data.py**.
    - **run_bq_create_dates_table:** Runs the **dates_table** function from **csr_bq_transform_data.py** to create the dates table for Power BI.
   
- **csr_bq_transform_data.py:** This script contains the following functions for preparing and transforming the data.
  - **transform_sr_data:** Below are some of the main transformations this function applies.
    - Remove "Aircraft Noise Complaint" SRs (~17% of all SRs).
    - Remove "311 INFORMATION ONLY CALL" SRs (~36% of all SRs).
    - Remove data that do not have latitude or longitude information (~0.05% of data).
    - Create new attribute 'Completion Time in Days' for SRs.
    - Join the SRs data table with the categories data table.
    - There are some SRs that were completed extremely quickly. For example, multiple graffiti removal requests at the same location were completed in less than a day. I believe most of these are instances of multiple people reporting the same issue, so these repeated requests were quickly closed. To work around this, I group the SR data by Latitude, Longitude, Created Date, SR Category, SR Sub-Category, and SR Type. Then, for each grouping that contains completed SRs, I aggregate by the maximum "Completion Time in Days" along with the associated SR Number, Status ("Completed"), Street Address, and Area Number. For groupings that contain open SRs, "Completion Time in Days" is null, Status is "Open", and the rest of the attributes are the first available value.
    - Based on the completion times computed above, I create new columns "Closed Date"
    - Created "Open SR Time in Days" for open SRs, which is the number of days between "Created Date" and date that this pipeline was executed.
  - **community_areas:** This formats the sides and community areas data table by title-casing the names and adding 2022 population data for each area.
  - **dates_table:** Creates table of dates that covers the range of the SR data, and adds columns such as month name, year, date of first day of week, date of first day of month, etc.
 
- As of Mar. 17, 2025, the raw SRs data from the past two years contained 3,689,117 data points. The processed SRs data contained 2,095,197 rows.
- The processed dataset are shown in the PBI data model below:
- <image>

## Summary of Findings

Below are some general insights.

- There is a general trend where service requests are more common during the warmer months and dip during the winter months.
- Over the past two years, the West Side saw the most service requests, especially in the summer of 2023 in the Austin community area. This was driven by service requests in the “Transportation and Street” and “Home and Buildings” categories, and more specifically due to requests that were “Water in Basement Complaint” types.
- In terms of service requests per capita, the Far Southeast Side had the highest over the past two years. In terms of average completion time of a service request, the South Side had the longest.
- The Far North and South Sides see “Home and Buildings” as the most common service request category, while “Transportation and Streets” is the most common category in the other sides. 

Here are some more specific insights that the city could use to improve services.

- The service request type “Buildings – Plumbing Violations” takes on average 92.22 days to complete. This has been improving since summer of 2024.
- “E-Scooter Parking” complaints have increased more than two-fold from 2023 to 2024, mostly driven by increases in the North Side. Some sort of enforcement or resource management measure is recommended to decrease these complaints in the summer and fall of 2025.
  - Below is a line chart of this trend across the city:
  - <image>
  - The maps below visualize the number of complaints of this request type in 2023 and 2024, respectively.
  - <image> 
  - <image>
  



