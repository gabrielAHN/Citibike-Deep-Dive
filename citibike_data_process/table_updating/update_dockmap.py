import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_dockmap(**kwargs):
    main_table = kwargs.get('name', 'DockTable')
    conn = kwargs.get('conn')

    temp_table = f"{main_table}_temp"
    create_temp_table(conn, temp_table)

    last_aggregated_time = kwargs.get('last_aggregated_time', None)

    process_citibike_data_chunked_by_year(
        conn,
        new_table="ImportedTable",
        temp_table=temp_table,
        last_aggregated_time=last_aggregated_time
    )

    unify_station_years_unique_station_name(conn, temp_table)
    
    finalize_docktable(conn, main_table, temp_table)

    drop_temp_table(conn, temp_table)

def create_temp_table(conn, temp_table_name):
    conn.execute(f'''
        CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} (
            station_name TEXT,
            station_id TEXT,
            station_lat REAL,
            station_lon REAL,
            station_data JSON
        )
    ''')

def drop_temp_table(conn, temp_table_name):
    conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")

def process_citibike_data_chunked_by_year(conn, new_table, temp_table, last_aggregated_time=None):
    time_filter = f"AND start_time > '{last_aggregated_time}'" if last_aggregated_time else ""
    all_years = conn.execute(
        f"SELECT DISTINCT year FROM {new_table} {time_filter} ORDER BY year"
    ).fetchall()

    for (yr,) in all_years:
        conn.execute(f"""
            INSERT INTO {temp_table} (station_name, station_id, station_lat, station_lon, station_data)
            WITH starts AS (
                SELECT
                    start_station_name AS station_name,
                    ANY_VALUE(start_station_id) AS station_id,
                    ANY_VALUE(start_station_latitude) AS station_lat,
                    ANY_VALUE(start_station_longitude) AS station_lon,
                    year,
                    month,
                    COUNT(*) AS starts_count
                FROM {new_table}
                WHERE year = '{yr}'
                  {time_filter}
                GROUP BY start_station_name, year, month
            ),
            ends AS (
                SELECT
                    end_station_name AS station_name,
                    ANY_VALUE(end_station_id) AS station_id,
                    ANY_VALUE(end_station_latitude) AS station_lat,
                    ANY_VALUE(end_station_longitude) AS station_lon,
                    year,
                    month,
                    COUNT(*) AS ends_count
                FROM {new_table}
                WHERE year = '{yr}'
                  {time_filter}
                GROUP BY end_station_name, year, month
            ),
            unioned AS (
                SELECT 
                    station_name,
                    COALESCE(starts.station_id, ends.station_id) AS station_id,
                    COALESCE(starts.station_lat, ends.station_lat) AS station_lat,
                    COALESCE(starts.station_lon, ends.station_lon) AS station_lon,
                    year,
                    month,
                    COALESCE(starts.starts_count, 0) AS starts_count,
                    COALESCE(ends.ends_count, 0) AS ends_count
                FROM starts
                FULL OUTER JOIN ends
                USING (station_name, year, month)
                WHERE station_name IS NOT NULL
            ),
            monthly AS (
                SELECT
                    station_name,
                    station_id,  -- Already resolved by COALESCE in unioned
                    station_lat,
                    station_lon,
                    year,
                    month,
                    starts_count AS month_starts,
                    ends_count AS month_ends,
                    (starts_count + ends_count) AS month_total
                FROM unioned
            ),
            yearly AS (
                SELECT
                    station_name,
                    ANY_VALUE(station_id) AS station_id,  -- Safe, as it’s from monthly
                    ANY_VALUE(station_lat) AS station_lat,
                    ANY_VALUE(station_lon) AS station_lon,
                    year,
                    SUM(month_starts) AS year_starts,
                    SUM(month_ends) AS year_ends,
                    json_group_object(
                        month,
                        json_object(
                          'month_total', month_total,
                          'month_starts', month_starts,
                          'month_ends', month_ends
                        )
                    ) AS months_json
                FROM monthly
                GROUP BY station_name, year
            )
            SELECT
                station_name,
                station_id,
                station_lat,
                station_lon,
                json_object(
                  '{yr}',
                  json_object(
                    'year_starts', year_starts,
                    'year_ends', year_ends,
                    'months', months_json
                  )
                ) AS station_data
            FROM yearly
        """)

def unify_station_years_unique_station_name(conn, temp_table):
    conn.execute("DROP TABLE IF EXISTS temp_unified")
    conn.execute(f"""
        CREATE TABLE temp_unified AS
        SELECT
            station_name,
            ANY_VALUE(station_id) AS station_id,
            ANY_VALUE(station_lat) AS station_lat,
            ANY_VALUE(station_lon) AS station_lon,
            json_group_array(station_data) AS partial_json
        FROM {temp_table}
        WHERE station_name IS NOT NULL
        GROUP BY station_name
    """)
    
    rows = conn.execute("""
        SELECT station_name, station_id, station_lat, station_lon, partial_json
        FROM temp_unified
    """).fetchall()

    final_rows = []
    for s_name, s_id, s_lat, s_lon, json_array in rows:
        if not json_array or json_array.strip() == '[]':
            merged_obj = {}
        else:
            try:
                arr = json.loads(json_array)
            except Exception as e:
                logging.error(f"Error parsing JSON for station {s_name}: {json_array}")
                arr = []
            merged_obj = {}
            for year_item in arr:
                if isinstance(year_item, dict):
                    merged_obj.update(year_item)
        final_rows.append((s_name, s_id, s_lat, s_lon, json.dumps(merged_obj)))
    
    conn.execute("DROP TABLE temp_unified")
    conn.execute(f"DROP TABLE {temp_table}")
    conn.execute(f"""
        CREATE TABLE {temp_table} (
            station_name TEXT,
            station_id TEXT,
            station_lat REAL,
            station_lon REAL,
            station_data JSON
        )
    """)
    conn.executemany(f"""
        INSERT INTO {temp_table} (station_name, station_id, station_lat, station_lon, station_data)
        VALUES (?, ?, ?, ?, ?)
    """, final_rows)

def finalize_docktable(conn, dock_table_name, aggregated_table_name):
    conn.execute("DROP TABLE IF EXISTS temp_update")
    conn.execute(f"""
        CREATE TEMP TABLE temp_update AS
        SELECT 
            t.station_name,
            t.station_id,
            t.station_lat,
            t.station_lon,
            t.station_data AS new_station_data,
            COALESCE(d.station_data, '{{}}') AS existing_station_data
        FROM {aggregated_table_name} t
        LEFT JOIN {dock_table_name} d
        ON d.station_name = t.station_name
    """)

    rows = conn.execute("""
        SELECT station_name, station_id, station_lat, station_lon, existing_station_data, new_station_data
        FROM temp_update
    """).fetchall()

    final_rows = []
    for s_name, s_id, s_lat, s_lon, existing_data, new_data in rows:
        existing_json = json.loads(existing_data)
        new_json = json.loads(new_data)
        
        for year, new_year_data in new_json.items():
            if year not in existing_json:
                existing_json[year] = new_year_data
            else:
                existing_year_data = existing_json[year]
                existing_year_data['year_starts'] = new_year_data['year_starts']
                existing_year_data['year_ends'] = new_year_data['year_ends']
                existing_months = existing_year_data.get('months', {})
                new_months = new_year_data.get('months', {})
                existing_months.update(new_months)
                existing_year_data['months'] = existing_months
        
        final_rows.append((s_name, s_id, s_lat, s_lon, json.dumps(existing_json)))

    conn.execute("DROP TABLE IF EXISTS temp_update_final")
    conn.execute(f"""
        CREATE TEMP TABLE temp_update_final (
            station_name TEXT,
            station_id TEXT,
            station_lat REAL,
            station_lon REAL,
            station_data JSON
        )
    """)
    conn.executemany(f"""
        INSERT INTO temp_update_final (station_name, station_id, station_lat, station_lon, station_data)
        VALUES (?, ?, ?, ?, ?)
    """, final_rows)

    conn.execute(f"""
        INSERT INTO {dock_table_name} (station_name, station_id, station_lat, station_lon, station_data)
        SELECT station_name, station_id, station_lat, station_lon, station_data
        FROM temp_update_final
        WHERE station_name NOT IN (SELECT station_name FROM {dock_table_name})
    """)

    conn.execute(f"""
        UPDATE {dock_table_name}
        SET 
            station_data = (
                SELECT t.station_data
                FROM temp_update_final t
                WHERE t.station_name = {dock_table_name}.station_name
            ),
            station_id = (
                SELECT t.station_id
                FROM temp_update_final t
                WHERE t.station_name = {dock_table_name}.station_name
            ),
            station_lat = (
                SELECT t.station_lat
                FROM temp_update_final t
                WHERE t.station_name = {dock_table_name}.station_name
            ),
            station_lon = (
                SELECT t.station_lon
                FROM temp_update_final t
                WHERE t.station_name = {dock_table_name}.station_name
            )
        WHERE station_name IN (SELECT station_name FROM temp_update_final)
    """)