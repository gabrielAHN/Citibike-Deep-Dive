import os
import json
import logging
import time
import datetime
import requests

import geopy.distance
import pandas as pd
import numpy as np

from ..shared_util.multi_threading import parallel_execute
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAPBOX_URL = 'https://api.mapbox.com/directions/v5/mapbox/cycling/{from_lon},{from_lat};{to_lon},{to_lat}'


def update_tripsmap(**kwargs):
    conn = kwargs.get('conn')
    main_table = kwargs.get('name')
    workers = kwargs.get('workers')

    temp_table = f"{main_table}_temp"
    new_table = 'ImportedTable'

    top_trips = get_top_ride_table(conn, new_table)

    add_trip_shapes(conn, temp_table, top_trips, workers)
    update_table(conn, main_table, temp_table)


def update_table(conn, main_table, temp_table):
    conn.execute(f"""
        UPDATE {main_table} AS t
        SET trip_count = t.trip_count + s.trip_count
        FROM {temp_table} AS s
        WHERE t.year = s.year
        AND t.from_station = s.from_station
        AND t.to_station = s.to_station
    """)
    conn.execute(f"""
        INSERT INTO {main_table}
        SELECT s.*
        FROM {temp_table} s
        LEFT JOIN {main_table} t 
        ON t.year = s.year
        AND t.from_station = s.from_station
        AND t.to_station = s.to_station
        WHERE t.year IS NULL
    """)
    conn.unregister(temp_table)


def get_top_ride_table(conn, new_table):
    top_trips = conn.execute(f'''
        WITH trip_aggregates AS (
            SELECT 
                year,
                MIN(start_time) AS trip_time,
                start_station_name AS from_station,
                start_station_latitude AS from_lat,
                start_station_longitude AS from_lon,
                end_station_name AS to_station,
                end_station_latitude AS to_lat,  
                end_station_longitude AS to_lon,
                rideable_type,
                COUNT(*) AS trip_count
            FROM "{new_table}"
            WHERE 
                start_station_name != end_station_name
            GROUP BY
                year, 
                start_station_name, 
                start_station_latitude, 
                start_station_longitude,
                end_station_name, 
                end_station_latitude, 
                end_station_longitude,
                rideable_type
        )
        SELECT 
            *
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY year ORDER BY trip_count DESC) AS rn
            FROM trip_aggregates
        ) ranked
        WHERE rn <= 30
        ORDER BY year, rn;
    ''').fetchall()
    return top_trips


def add_trip_shapes(conn, temp_table, trip_list, workers):
    batch_size = 10
    row_data = []

    for i in range(0, len(trip_list), batch_size):
        batch = trip_list[i:i + batch_size]
        batch_results = parallel_execute(request_trip_shape, batch, workers)
        row_data.extend(batch_results)

    columns = [
        'year', 'rideable_type', 'from_station',
        'to_station', 'trip_count', 'waypoints'
    ]
    combined_df = pd.DataFrame(row_data, columns=columns)

    combined_df['waypoints'] = combined_df['waypoints'].apply(json.dumps)

    conn.register(temp_table, combined_df)
    return combined_df


def request_trip_shape(row, max_retries=3):
    query_params = {
        "geometries": "geojson",
        "access_token": os.getenv('MAPBOX_API_KEY')
    }

    trip_url = MAPBOX_URL.format(
        from_lon=row[4],
        from_lat=row[3],
        to_lon=row[7],
        to_lat=row[6]
    )

    for attempt in range(max_retries):
        response = requests.get(trip_url, params=query_params)

        if response.status_code == 200:
            shape_data = response.json()
            final_shape = get_trip_times(
                shape=shape_data['routes'][0]['geometry']['coordinates'],
                start_time=row[1]
            )
            break
        else:
            logger.warning(
                f"API request failed (attempt {attempt + 1}/{max_retries}): "
                f"Status code {response.status_code}: {response.text}"
            )
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                logger.error(f"Max retries exceeded for trip: {row}")
                final_shape = []

    return [row[0], row[8], row[2], row[5], row[9], final_shape]


def get_distance(lat1, lon1, lat2, lon2):
    distance = geopy.distance.geodesic(
        (lat1, lon1),
        (lat2, lon2)
    ).km
    return distance


def divide_points(lat1, lon1, lat2, lon2, num_sections):
    start = np.array([lon1, lat1])
    end = np.array([lon2, lat2])
    delta = end - start
    steps = np.linspace(0, 1, num_sections)[:, np.newaxis]
    points = start + steps * delta
    points = np.round(points, 5)
    return points.tolist()


def extend_shape(shape):
    extended_shape = []

    for i in range(len(shape) - 1):
        lon1, lat1 = shape[i]
        lon2, lat2 = shape[i+1]

        distance = get_distance(lon1, lat1, lon2, lat2)

        if 0.2 < distance < 0.37:
            between_points = divide_points(lat1, lon1, lat2, lon2, 2)
            extended_shape.extend(between_points)
        elif distance > 0.37:
            between_points = divide_points(lat1, lon1, lat2, lon2, 4)
            extended_shape.extend(between_points)
        else:
            extended_shape.append([lon1, lat1])

    extended_shape.append([lon2, lat2])

    extended_shape = [
        [
            round(y, 5)
            for y in i
        ]
        for i in extended_shape
    ]
    return extended_shape

def get_time_of_day_seconds(dt):
    return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6


def get_trip_times(shape, start_time):
    shape = extend_shape(shape)

    trip_times = []
    for i, (lon, lat) in enumerate(shape):
        current_time = start_time + i * datetime.timedelta(hours=1)
        trip_times.append(
            {
                'timestamp': get_time_of_day_seconds(current_time),
                'coordinates': [lon, lat],
            }
        )
    return trip_times
