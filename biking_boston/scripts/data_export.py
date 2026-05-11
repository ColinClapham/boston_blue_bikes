import snowflake.connector
import os
from biking_boston.scripts.utils import connect_to_snowflake

def load_private_key():
    key_path = "/tmp/rsa_key.pem"
    with open(key_path, "w") as f:
        f.write(os.environ["PEM_FILE_CONTENTS"])
    return key_path

def export_to_snowflake_staging(month_string):

    # file_path = f"../outputs/raw/blue_bikes_trips_data_raw_{month_string}.parquet"
    file_path = "/tmp/blue_bikes_trips_data_raw_{month_string}.parquet"

    conn = connect_to_snowflake()

    cur = conn.cursor()

    # # 1. Create a stage (one-time safe to re-run)
    # cur.execute("""
    #     CREATE OR REPLACE STAGE trips_parquet_stage
    #     FILE_FORMAT = (TYPE = PARQUET)
    # """)

    # 2. Upload file to stage
    cur.execute(f"""
        PUT file://{file_path} @trips_parquet_stage AUTO_COMPRESS=FALSE
    """)

    cur.close()
    conn.close()


def copy_staging_to_raw():
    conn = connect_to_snowflake()

    cur = conn.cursor()

    # Load into table
    cur.execute("""
            COPY INTO TRIPS
            FROM @trips_parquet_stage
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = FALSE;
        """)

    cur.close()
    conn.close()

