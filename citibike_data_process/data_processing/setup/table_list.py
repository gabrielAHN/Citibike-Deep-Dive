from ...shared_util.citibike_objects import TableObject
from ...table_updating.update_dockmap import update_dockmap
from ...table_updating.update_tripsmap import update_tripsmap
from ...table_updating.update_heatmap import update_heatmap
from ...table_updating.update_linegraph import update_linegraph
from ...table_updating.update_status_data import update_data_table


table_list = [
    TableObject(
        name='LineGraphTable',
        sql_query="""
            CREATE TABLE IF NOT EXISTS LineGraphTable (
                year TEXT,
                month TEXT,
                subscriber_count INTEGER,
                customer_count INTEGER
            )
        """,
        update_function=update_linegraph
    ),
    TableObject(
        name='HeatMapTable',
        sql_query="""
            CREATE TABLE IF NOT EXISTS HeatMapTable (
                year TEXT,
                month TEXT,
                hour INTEGER,
                total_count INTEGER
            )
        """,
        update_function=update_heatmap
    ),
    TableObject(
        name='DockTable',
        sql_query="""
            CREATE TABLE IF NOT EXISTS DockTable (
                station_name TEXT,
                station_id TEXT,
                station_lat REAL,
                station_lon REAL,
                station_data JSON
            )
        """,
        update_function=update_dockmap
    ),
    TableObject(
        name='TripTable',
        sql_query="""
            CREATE TABLE IF NOT EXISTS TripTable (
                year TEXT,
                rideable_type TEXT,
                from_station TEXT,
                to_station TEXT,
                trip_count INTEGER,
                waypoints JSON
            )
        """,
        update_function=update_tripsmap
    ),
    TableObject(
        name='StatusDataTable',
        sql_query="""
            CREATE TABLE IF NOT EXISTS StatusDataTable (
                year INTEGER,
                month INTEGER,
                complete BOOLEAN
            )
        """,
        update_function=update_data_table
    )
]

