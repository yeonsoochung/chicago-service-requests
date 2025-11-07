"""
Chicago Service Requests: BigQuery Data Processing
1. Process service requests data frame and create dates data frame
2. Process community areas data frame
"""
from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import date

def transform_sr_data():
    """ Fetch data from BigQuery, transform it with Pandas, and load it back to BQ """
    client = bigquery.Client()
    project_id = "csr-project-453302"
    dataset_id = "chicago_service_requests"
    source_table_sr = "csr_raw"
    source_table_cats = "sr_categories"
    destination_table_dates = "dates"
    destination_table_sr = "csr_processed"
    
    # SQL Queries to fetch data
    sr_query = f"SELECT * FROM `{project_id}.{dataset_id}.{source_table_sr}`"
    cats_query = f"SELECT * FROM `{project_id}.{dataset_id}.{source_table_cats}`"
    
    # Load data into Pandas DataFrame
    sr_df = client.query(sr_query).to_dataframe()
    sr_cats_df = client.query(cats_query).to_dataframe()

    sr_df['created_date'] = pd.to_datetime(sr_df['created_date'])
    start_date = sr_df['created_date'].min().date()
    end_date = sr_df['created_date'].max().date()

    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Create the date table
    dates_df = pd.DataFrame({
        'Date': [d.date() for d in date_range],  # Convert to plain date using list comprehension
        'Month Name': date_range.strftime('%B'),
        'Month': date_range.month,
        'Year': date_range.year,
        'Quarter': np.where(date_range.month.isin([1, 2, 3]), 'Q1',
                    np.where(date_range.month.isin([4, 5, 6]), 'Q2',
                    np.where(date_range.month.isin([7, 8, 9]), 'Q3', 'Q4'))),
        'Week Start': [(d - pd.Timedelta(days=(d.weekday() + 1) % 7)).date() for d in date_range],  # Convert to plain date
        'Month Start': [d.to_period('M').start_time.date() for d in date_range],  # Convert to plain date
        'Quarter Start': [d.to_period('Q').start_time.date() for d in date_range],  # Convert to plain date
        'Year Start': [d.to_period('Y').start_time.date() for d in date_range]  # Convert to plain date
    })
    
    # Define the schema and job config (WRITE_TRUNCATE to overwrite)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Upload transformed data to a new BigQuery table
    dates_table_ref = f"{project_id}.{dataset_id}.{destination_table_dates}"
    client.load_table_from_dataframe(dates_df, dates_table_ref, job_config=job_config)
    
    ''' Transform SR data '''
    # Make sure columns are in correct order
    sr_df = sr_df[['sr_number', 'sr_type', 'sr_short_code', 'owner_department', 'status', 
                   'origin', 'created_date', 'last_modified_date', 'closed_date', 'street_address',
                   'street_direction', 'street_name', 'street_type', 'community_area',
                   'created_hour', 'created_day_of_week', 'created_month', 'latitude', 'longitude']]

    # Only include SR's that are in sr_cats_df
    sr_df = sr_df[sr_df['sr_type'].isin(sr_cats_df['sr_type'])]

    # Remove 'Aircraft Noise Complaint' from sr data
    sr_df = sr_df[sr_df['sr_type'] != 'Aircraft Noise Complaint']

    # Remove rows where latitude or longitude is nan
    sr_df = sr_df.dropna(subset=["latitude", "longitude"])

    # Title-case street_name and street_type
    sr_df[['street_address', 'street_name', 'street_type']] = \
        sr_df[['street_address', 'street_name', 'street_type']].apply(lambda col: col.str.title())
    
    # Add SR completion time column and place it after 'closed_date' column
    sr_df['created_date'] = pd.to_datetime(sr_df['created_date'].astype(str)).dt.date
    sr_df['last_modified_date'] = pd.to_datetime(sr_df['last_modified_date'].astype(str)).dt.date
    sr_df['closed_date'] = pd.to_datetime(sr_df['closed_date'].astype(str)).dt.date
    sr_df['sr_completion_time'] = pd.to_timedelta(sr_df['closed_date'] - sr_df['created_date'])
    sr_df.insert(9, 'sr_completion_time', sr_df.pop('sr_completion_time'))

    # Replace 'created_day_of_week' with day names
    day_mapping = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 
                   6: 'Friday', 7: 'Saturday'}
    sr_df['created_day_of_week'] = sr_df['created_day_of_week'].map(day_mapping)

    # Rename sr_df columns names
    sr_df.columns = ['SR Number', 'SR Type', 'SR Code', 'Owner Department', 'Status', 'Origin',
                     'Created Date', 'Last Modified Date', 'Closed Date', 'Completion Time', 
                     'Street Address', 'Street Direction', 'Street Name', 'Street Type', 
                     'Area Number', 'Created Hour', 'Created Day of Week', 
                     'Created Month', 'Latitude', 'Longitude']

    # Exclude 'Canceled' or 'Closed' statuses
    sr_df = sr_df[sr_df['Status'].isin(['Open', 'Completed'])]

    # Create column for completion time in days
    sr_df['Completion Time in Days'] = sr_df['Completion Time'].dt.total_seconds() / (3600*24)

    # Rename sr_cats_df columns names
    sr_cats_df.columns = ['SR Category', 'SR Sub-Category', 'SR Type']

    # Join sr_df with sr_cats_df and select the columns of interest
    sr_join = pd.merge(sr_df, sr_cats_df, on='SR Type')
    sr_join = sr_join[['SR Number', 'SR Type', 'Status', 'Created Date', 'Closed Date', 'Completion Time in Days', 
                        'SR Category', 'SR Sub-Category', 'Street Address', 'Area Number', 
                        'Latitude', 'Longitude']]

    # Define the columns to group by
    group_cols = ['Latitude', 'Longitude', 'Created Date', 'SR Category', 'SR Sub-Category', 'SR Type']

    # Get the nan completion times and group those to get unique SRs and mark their status as 'Open'
    sr_join_na = sr_join[sr_join['Completion Time in Days'].isna()]
    sr_grouped_na = sr_join_na.groupby(group_cols).agg({
        'Completion Time in Days': 'first',
        'SR Number': 'first',  # First SR Number
        'Status': lambda x: 'Open',
        'Street Address': 'first',  # First street address
        'Area Number': 'first'  # First area number
    }).reset_index()

    # Function to get the row with max Completion Time in Days
    def get_max_row(group):
        max_row = group.loc[group['Completion Time in Days'].idxmax()]
        return pd.Series({
            'Completion Time in Days': max_row['Completion Time in Days'],
            'SR Number': max_row['SR Number'],
            'Status': 'Completed',
            'Street Address': max_row['Street Address'],
            'Area Number': max_row['Area Number']
        })

    # Group and apply the function on sr_join where completion time is not nan
    sr_join_notna = sr_join[sr_join['Completion Time in Days'].notna()]
    sr_grouped_notna = sr_join_notna.groupby(group_cols).apply(get_max_row).reset_index()

    sr_grouped = pd.concat([sr_grouped_notna, sr_grouped_na], axis=0)
    sr_grouped = sr_grouped.sort_values('Created Date')

    # Round completion time to nearest day
    sr_grouped['Completion Time in Days'] = np.round(sr_grouped['Completion Time in Days'])

    # Compute Average Closed Date (Created Date + Avg Completion Time)
    sr_grouped['Closed Date'] = sr_grouped.apply(
        lambda row: row['Created Date'] + pd.to_timedelta(row['Completion Time in Days'], unit='D') 
        if not pd.isna(row['Completion Time in Days']) else np.nan, axis=1
    )

    # Open service requests: how long have they been open?
    today_date = date.today()
    sr_grouped['Open SR Time in Days'] = sr_grouped.apply(
        lambda row: today_date - row['Created Date'] 
        if row['Status'] == 'Open' else np.nan, axis=1
    )
    sr_grouped['Open SR Time in Days'] = pd.to_timedelta(sr_grouped['Open SR Time in Days']).dt.days.astype(float)
    # sr_grouped['Open SR Time in Days'] = sr_grouped['Open SR Time in Days'].days.astype(float)
    
    # Define the schema and job config (WRITE_TRUNCATE to overwrite)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Upload transformed data to a new BigQuery table
    sr_table_ref = f"{project_id}.{dataset_id}.{destination_table_sr}"
    client.load_table_from_dataframe(sr_grouped, sr_table_ref, job_config=job_config)
    


def community_areas():
    client = bigquery.Client()
    project_id = "csr-project-453302"
    dataset_id = "chicago_service_requests"
    source_table = "community_areas"
    destination_table = "community_areas_processed"
    
    # SQL Query to fetch data
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{source_table}`"
    
    # Load data into Pandas DataFrame
    comm_areas_df = client.query(query).to_dataframe()

    # Transform data
    # Rename comm_areas_df columns and sort by area number
    comm_areas_df.rename(columns={'AREA_NUMBE': 'Area Number', 'COMMUNITY': 'Community Area'}, inplace=True)
    comm_areas_df = comm_areas_df.sort_values(by='Area Number')
    comm_areas_df['Community Area'] = comm_areas_df['Community Area'].str.title()

    # Add population columns to comm_areas_df
    comm_areas_df['Population'] = [55711, 79265, 57464, 42271, 35814, 101739, 69099, 101208, 11402, 39671, 
                                  27032, 19855, 19398, 47663, 64213, 53584, 42574, 13765, 73991, 23157, 
                                  36374, 71384, 55416, 86350, 100120, 16374, 19901, 65581, 30409, 69708, 
                                  34237, 41671, 28216, 13193, 21355, 6977, 2233, 24813, 18637, 12366, 
                                  29559, 24223, 55396, 31341, 9647, 30315, 2246, 12087, 37610, 6856, 
                                  15218, 23942, 25449, 7817, 9025, 36401, 13867, 42243, 15479, 33221, 
                                  40957, 18366, 34788, 24728, 32594, 52464, 25772, 21378, 28991, 42745, 
                                  46468, 19781, 26456, 19121, 20718, 14024, 56099]

    sides_dict = {'Far North Side': [76,9,10,11,12,13,14,2,4,3,1,77], 
                  'Northwest Side': [17,18,19,15,16,20], 
                  'North Side': [21,22,5,6,7], 
                  'West Side': [25,23,24,26,27,28,29,30,31], 
                  'Central': [8,32,33], 
                  'South Side': [60,34,35,37,38,36,39,40,41,42,69,43], 
                  'Southwest Side': [56,64,57,62,65,58,63,66,59,61,67,68], 
                  'Far Southwest Side': [70,71,72,73,74,75], 
                  'Far Southeast Side': [44,45,47,48,46,49,50,51,52,53,54,55]}
    area_to_side = {area: side for side, areas in sides_dict.items() for area in areas}
    comm_areas_df['Side'] = comm_areas_df['Area Number'].map(area_to_side)
    
    # Define the schema and job config (WRITE_TRUNCATE to overwrite)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Upload transformed data to a new BigQuery table
    table_ref = f"{project_id}.{dataset_id}.{destination_table}"
    client.load_table_from_dataframe(comm_areas_df, table_ref, job_config=job_config)






