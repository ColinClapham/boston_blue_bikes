import requests
import zipfile
import io
import pandas as pd
from biking_boston.scripts.utils import iter_months, get_date_range, parquet_exists, make_ride_id
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import re
from data_export import export_to_snowflake_staging, copy_staging_to_raw


def stitch_dataframes_vertically(dataframes):
    # Concatenate the DataFrames vertically
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df


def download_and_unzip_csv(url, zip_file_name, csv_file_name):
    # Step 1: Download the ZIP file from the URL
    response = requests.get(url)

    # Step 2: Unzip the downloaded ZIP file
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        csv_name = [f for f in zip_file.namelist() if f.endswith(".csv")][0]

        with zip_file.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file)

        if "rideable_type" not in df.columns:
            df["rideable_type"] = "classic_bike"
        else:
            df["rideable_type"] = df["rideable_type"].fillna("classic_bike")

        # Step 6: Process Data to account for migration of data points
        if 'ride_id' in df:
            df = df[['started_at',
                     'ended_at',
                     'start_station_name',
                     'start_station_id',
                     'end_station_name',
                     'end_station_id',
                     'start_lat',
                     'start_lng',
                     'end_lat',
                     'end_lng',
                     'member_casual',
                     'rideable_type']]
        else:
            df = df[['starttime',
                     'stoptime',
                     'start station name',
                     'start station id',
                     'end station name',
                     'end station id',
                     'start station latitude',
                     'start station longitude',
                     'end station latitude',
                     'end station longitude',
                     'usertype',
                     'rideable_type']].rename(columns={'starttime':'started_at',
                                                       'stoptime':'ended_at',
                                                       'start station name': 'start_station_name',
                                                       'start station id':'legacy_start_station_id',
                                                       'end station name':'end_station_name',
                                                       'end station id':'legacy_end_station_id',
                                                       'start station latitude':'start_lat',
                                                       'start station longitude':'start_lng',
                                                       'end station latitude':'end_lat',
                                                       'end station longitude':'end_lng',
                                                       'usertype':'member_casual'})

        df["ride_id"] = df.apply(make_ride_id, axis=1)

        return df


def extract_trip_data():
    start_month, end_month = get_date_range()
    blue_bikes_trip_data = pd.DataFrame()
    for y, m in iter_months(start_month, end_month):
        month = "%d%02d" % (y, m)

        # 👇 skip if already processed
        if parquet_exists(month):
            logger.info(f"Skipping {month}, already exists")
            continue

        logger.info(f'Reading month {month}')

        # possible filename patterns
        candidates = [
            f"{month}-bluebikes-tripdata.zip",
            f"{month}-bluebikes-tripdata.csv.zip",
        ]

        success = False

        for file_name in candidates:
            url = f"https://s3.amazonaws.com/hubway-data/{file_name}"

            try:
                df = download_and_unzip_csv(
                    url,
                    zip_file_name=file_name,
                    csv_file_name=file_name.replace(".zip", "")
                )
                # 👇 Extract YYYYMM directly from filename
                match = re.search(r"\d{6}", file_name)
                yyyymm = match.group(0)

                output_path = f"../outputs/raw/blue_bikes_trips_data_raw_{yyyymm}.parquet"
                df.to_parquet(output_path, index=False)

                logger.info(f"Wrote {output_path}")
                success = True

                export_to_snowflake_staging(yyyymm)
                logger.info(f"Wrote blue_bikes_trips_data_raw_{yyyymm} to snowflake")
                break

            except Exception as e:
                logger.warning(f"Failed for {file_name}: {e}")

        if not success:
            raise ValueError(f"No valid file found for month {month}")

    return blue_bikes_trip_data


def extract_hub_data():
    blue_bikes_trip_data = pd.DataFrame()
    logger.info(f'Reading Hub Data')
    url = f'https://s3.amazonaws.com/hubway-data/current_bluebikes_stations.csv'
    hub_data_df = pd.read_csv(url,skiprows=1)
    logger.info(f'Hub Data Complete!')
    return hub_data_df


if __name__ == '__main__':

    is_read_trip_data = True
    # is_read_hub_data = False

    if is_read_trip_data:
        logger.info('Reading Trip Data')
        extract_trip_data()
        logger.info(f"Data has been written to output path in Parquet format.")
        copy_staging_to_raw()
        logger.info('Copied staged data into TRIPS table')

    else:
        logger.info('Skip Trip Data')
        pass

    # if is_read_hub_data:
    #     logger.info('Reading Hub Data')
    #     extract_hub_data().to_csv('../inputs/blue_bikes_hub_data.csv')
    # else:
    #     logger.info('Skip Hub Data')
    #     pass
