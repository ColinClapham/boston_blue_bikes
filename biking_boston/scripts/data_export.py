import snowflake.connector

def export_to_snowflake_staging(month_string):

    file_path = f"../outputs/raw/blue_bikes_trips_data_raw_{month_string}.parquet"

    conn = snowflake.connector.connect(
        user="COLINCLAPHAM",
        account="TMHSYSP-WZC86394",
        warehouse="bluebikes_prod",
        database="BLUEBIKES",
        schema="RAW",
        private_key_file="../../rsa_key.pem"
    )

    cur = conn.cursor()

    # 1. Create a stage (one-time safe to re-run)
    cur.execute("""
        CREATE OR REPLACE STAGE trips_parquet_stage
        FILE_FORMAT = (TYPE = PARQUET)
    """)

    # 2. Upload file to stage
    cur.execute(f"""
        PUT file://{file_path} @trips_parquet_stage AUTO_COMPRESS=FALSE
    """)

    cur.close()
    conn.close()


def copy_staging_to_raw():
    conn = snowflake.connector.connect(
        user="COLINCLAPHAM",
        account="TMHSYSP-WZC86394",
        warehouse="bluebikes_prod",
        database="BLUEBIKES",
        schema="RAW",
        private_key_file="../../rsa_key.pem"
    )

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

