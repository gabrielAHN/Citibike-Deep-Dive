import logging

from ...shared_util.parser import parse_file_date


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

user_types = {
    'member': 'subscriber',
    'casual': 'customer'
}

StationBlackList = [
    '8D QC Station 01', 'SSP - Basement',
    "NYCBS Depot - STY - Valet Scan",
    "333 Johnson TEST 1", "8D Mobile 01",
    "8D OPS 01"
]

StationBlackListSQL = "(" + ", ".join([f"'{station}'" for station in StationBlackList]) + ")"

user_type_case = "CASE " + " ".join(
    f"WHEN lower(user_type) = '{old}' THEN '{new}'"
    for old, new in user_types.items()
) + " ELSE user_type END"


def db_import(conn, new_data):
    table_name = "ImportedTable"

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_import_table(conn, table_name, new_data)

    pandas_df = new_data.file

    year, month = parse_file_date(new_data.table_name)

    conn.register("temp_table", pandas_df)
    conn.execute(
    f"""
        INSERT INTO "{table_name}"
            SELECT
                * 
                REPLACE (
                    CASE
                        WHEN start_station_latitude BETWEEN 40.478245113529454 AND 40.98852055760176
                            AND start_station_longitude BETWEEN -74.25383719445274 AND -73.59127683334592
                        THEN start_station_latitude
                        ELSE start_station_longitude
                    END AS start_station_latitude,
                    CASE
                        WHEN start_station_latitude BETWEEN 40.478245113529454 AND 40.98852055760176
                            AND start_station_longitude BETWEEN -74.25383719445274 AND -73.59127683334592
                        THEN start_station_longitude
                        ELSE start_station_latitude
                    END AS start_station_longitude,
                    CASE
                        WHEN end_station_latitude BETWEEN 40.478245113529454 AND 40.98852055760176
                            AND end_station_longitude BETWEEN -74.25383719445274 AND -73.59127683334592
                        THEN end_station_latitude
                        ELSE end_station_longitude
                    END AS end_station_latitude,
                    CASE
                        WHEN end_station_latitude BETWEEN 40.478245113529454 AND 40.98852055760176
                            AND end_station_longitude BETWEEN -74.25383719445274 AND -73.59127683334592
                        THEN end_station_longitude
                        ELSE end_station_latitude
                    END AS end_station_longitude,
                    lower({user_type_case}) AS user_type
                ),
                strftime('%Y', start_time) AS year,
                strftime('%b', start_time) AS month
            FROM temp_table
            WHERE
                start_station_name != ''
                AND start_station_id != ''
                AND start_station_latitude != 0
                AND end_station_latitude != 0
                AND start_station_name NOT IN {StationBlackListSQL}
                AND end_station_name NOT IN {StationBlackListSQL}
                AND start_station_latitude IS NOT NULL
                AND start_station_longitude IS NOT NULL
                AND end_station_latitude   IS NOT NULL
                AND end_station_longitude  IS NOT NULL
                AND strftime('%Y', start_time) = '{year}'
    """)
    conn.unregister("temp_table")
    logging.info(f"💾 imported {new_data.table_name}")

    conn.execute("""
        CREATE INDEX IF NOT EXISTS year_month_index 
        ON ImportedTable (year, month);
    """)


def create_import_table(conn, table_name, data_object):
    conn.register("temp_table", data_object.file)

    conn.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS "{table_name}" AS
        SELECT
            * 
            REPLACE (rideable_type::STRING AS rideable_type),
            strftime('%Y', start_time) AS year,
            strftime('%b', start_time) AS month
        FROM temp_table
        LIMIT 0
    """)
    conn.unregister("temp_table")
