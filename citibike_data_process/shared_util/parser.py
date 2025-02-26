import re
    

def create_table_name(table_name, filename: str) -> str:
    match = re.match(r"^(\d{4})(\d{2})?-citibike-tripdata(\.csv)?\.zip$", filename)
    if match.group(1):
        table_name += f'_{match.group(1)}'
    if match.group(2):
        table_name += f'_{match.group(2)}'
    return table_name


def parse_year(filename: str) -> str:
    match = re.match(r"^(\d{4})(\d{2})?-citibike-tripdata(\.csv)?\.zip$", filename)
    if match:
        return match.group(1)
    return ""


def parse_file_date(fileObject: str):
    match = re.match(r"^Citibike_(\d{4})(?:_(\d{2}))?$", fileObject)
    if match:
        year = match.group(1)
        month = match.group(2) if match.group(2) else None
        return year, month

    return False

def parse_file_name(filename: str):
    match = re.match(r"^(\d{4})(\d{2})?-citibike-tripdata(\.csv)?\.zip$", filename)
    if match:
        year = match.group(1)
        month = match.group(2) if match.group(2) else None
        return year, month
    return False

def parse_file_date(file_name: str):
    match = re.match(r"^Citibike_(\d{4})(?:_(\d{2}))?$", file_name)
    if match:
        year = match.group(1)
        month = match.group(2) if match.group(2) else None
        return year, month
    return None, None