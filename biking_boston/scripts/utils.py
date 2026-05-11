import os
from datetime import datetime
import hashlib


def to_month(yyyymm):
    y, m = int(yyyymm[:4]), int(yyyymm[4:])
    return y * 12 + m

def iter_months(start, end):
    for month in range(to_month(start), to_month(end) + 1):
        y, m = divmod(month-1, 12)  # ugly fix to compensate
        yield y, m + 1              # for 12 % 12 == 0

def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File '{file_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"Error occurred while deleting the file: {e}")

def get_date_range():
    # start_month = "201805"
    start_month = "202501"

    today = datetime.today()
    year = today.year
    month = today.month

    # go back one month
    if month == 1:
        end_year = year - 1
        end_month = 12
    else:
        end_year = year
        end_month = month - 1

    end_month_str = f"{end_year}{end_month:02d}"

    return start_month, end_month_str


def parquet_exists(yyyymm):
    path = f"../outputs/raw/blue_bikes_trips_data_raw_{yyyymm}.parquet"
    return os.path.exists(path)


def get_csv_filename(file_name):
    if file_name.endswith(".csv.zip"):
        return file_name.replace(".csv.zip", ".csv")
    elif file_name.endswith(".zip"):
        return file_name.replace(".zip", ".csv")
    else:
        raise ValueError(f"Unexpected filename: {file_name}")

def make_ride_id(row):
    base_string = (
        str(row["started_at"]) +
        str(row["ended_at"]) +
        str(row["start_station_name"]) +
        str(row["end_station_name"]) +
        str(row.get("bike_id", ""))  # optional if exists
    )

    return hashlib.md5(base_string.encode()).hexdigest()