import requests
import zipfile
import io
import pandas as pd
from utils import iter_months, delete_file
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq

def stitch_dataframes_vertically(dataframes):
    # Concatenate the DataFrames vertically
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df
def download_and_unzip_csv(url, zip_file_name, csv_file_name):
    # Step 1: Download the ZIP file from the URL
    response = requests.get(url)

    # Step 2: Unzip the downloaded ZIP file
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        # Extract all the files from the ZIP
        zip_file.extractall()

        # Step 3: Find the CSV file inside the unzipped folder
        csv_file_path = zip_file_name.replace('.zip', '.csv')

        # Step 4: Read the CSV data using pandas
        logger.info(f'Storing {csv_file_name}')
        df = pd.read_csv(csv_file_path)

        #Step 5: Delete File from path
        logger.info(f'Removing {csv_file_name}')
        delete_file(csv_file_name)

        return df

def extract_trip_data(start_month, end_month):
    blue_bikes_trip_data = pd.DataFrame()
    for y, m in iter_months(start_month, end_month):
        month = "%d%02d" % (y, m)
        logger.info(f'Reading month {month}')
        url = f'https://s3.amazonaws.com/hubway-data/{month}-bluebikes-tripdata.zip'
        zip_file_name = f'{month}-bluebikes-tripdata.zip'
        csv_file_name = f'{month}-bluebikes-tripdata.csv'
        blue_bikes_trip_data = stitch_dataframes_vertically([download_and_unzip_csv(url,zip_file_name,csv_file_name),
                                                             blue_bikes_trip_data])
        logger.info(f'Month {month} Complete!')
    return blue_bikes_trip_data


# Define the path for the Parquet file
parquet_file = '../inputs/blue_bikes_master_trip_data.parquet'
# Convert the pandas DataFrame to a pyarrow Table
table = pa.Table.from_pandas(extract_trip_data('201805', '202305'))
# Write the Table to a Parquet file
pq.write_table(table, parquet_file)
print(f"Data has been written to '{parquet_file}' in Parquet format.")
