import re
import pyarrow as pa
import pyarrow.csv as pv


clean_dict = {
    'member_casual': 'user_type',
    'usertype': 'user_type',
    'start_lng': 'start_station_latitude',
    'start_lat': 'start_station_longitude',
    'end_lat': 'end_station_latitude',
    'end_lng': 'end_station_longitude',
    'started_at': 'start_time',
    'starttime': 'start_time',
    'stoptime' : 'end_time',
    'stop_time' : 'end_time',
    'ended_at': 'end_time'
}

SCHEMA = pa.schema([
    ("start_time", pa.timestamp("ns")),
    ("end_time", pa.timestamp("ns")),
    ("start_station_name", pa.string()),
    ("start_station_id", pa.string()),
    ("end_station_name", pa.string()),
    ("end_station_id", pa.string()),
    ("start_station_longitude", pa.float64()),
    ("start_station_latitude", pa.float64()),
    ("end_station_latitude", pa.float64()),
    ("end_station_longitude", pa.float64()),
    ("user_type", pa.string()),
    ('rideable_type', pa.string())
])

cols_to_keep = [
    'start_time', 'end_time','start_station_name',
    'start_station_id', 'end_station_name', 'end_station_id',
    'start_station_longitude', 'start_station_latitude',
    'end_station_latitude', 'end_station_longitude', 'user_type',
    'rideable_type'
]


def read_csv_from_bytes(byte_obj):
    parse_opts = pv.ParseOptions(delimiter=",")
    convert_opts = pv.ConvertOptions(
        timestamp_parsers=[
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M"
        ]
    )
    read_opts = pv.ReadOptions(autogenerate_column_names=False)
    
    table = pv.read_csv(
        byte_obj,
        parse_options=parse_opts,
        convert_options=convert_opts,
        read_options=read_opts
    )
    new_names = [cleanup_columns(col) for col in table.column_names]
    table = table.rename_columns(new_names)

    for col in cols_to_keep:
        if col not in table.column_names:
            field_type = SCHEMA.field(col).type
            table = table.append_column(col, pa.nulls(table.num_rows, type=field_type))
            
    table = table.select(cols_to_keep)
    return table.cast(SCHEMA)

def cleanup_columns(column):
    clean_column= re.sub(r'\s', '_', column).lower()
    clean_column= clean_dict.get(clean_column, clean_column)
    return clean_column
