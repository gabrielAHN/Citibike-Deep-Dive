import os

import tempfile
import requests
import logging

from bs4 import BeautifulSoup

from .new_file_check import new_file_check
from ...shared_util.parser import parse_file_date
from ...shared_util.s3_functions import download_file
from ...shared_util.multi_threading import parallel_file_upload


folder_path = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'data'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def retrieve_data(conn, args, workers):
    if args.file_remote:
        logging.info("Retrieving data remotely 🔗")
        new_data_list = get_remote_files(conn, args, workers)

    if args.file_local or args.read_none:
        logging.info("Retrieving data Locally 📁")
        new_data_list = get_local_files(conn, args, workers)

    if new_data_list:
        return sorted(new_data_list, key=sort_key)    


def get_local_files(conn, args, workers):
    from ...shared_util.citibike_objects import FileObject

    local_files = new_file_check(
        conn,
        args,
        [
            file
            for file in os.listdir(folder_path)
            if file.lower().endswith(".zip")
        ]
    )

    if local_files:
        return [
            FileObject(
                file=file,
                name=file,
                workers=workers
            )
            for file in local_files
        ]


def get_remote_files(conn, args, workers):
    from ...shared_util.citibike_objects import FileObject
    from ...shared_util.parser import parse_year

    response = requests.get('https://s3.amazonaws.com/tripdata/')
    soup = BeautifulSoup(response.content, 'xml')

    valid_files = new_file_check(
        conn,
        args,
        [
            date.text for date in soup.findAll("Key")
            if 'JC-' not in date.text
            and '.html' not in date.text
            and parse_year(date.text) >= '2013'
        ]
    )

    if (valid_files):
        zip_objects = parallel_file_upload(
            upload_zip_file, valid_files, workers, show_progress=True)

        return [
            FileObject(
                file=i['file_path'],
                name=i['name'],
                workers=workers
            )
            for i in zip_objects
        ]


def upload_zip_file(file_date):
    with tempfile.NamedTemporaryFile(suffix=".zip") as tmp_file:
        local_zip_path = tmp_file.name

    download_file(
        bucket='tripdata',
        file_date=file_date,
        file_name=local_zip_path
    )
    return { 'name': file_date ,'file_path': local_zip_path }


def sort_key(file_obj):
    year, month = parse_file_date(file_obj.table_name)
    if year is not None:
        return int(year) * 100 + (int(month) if month is not None else 0)
    return float('inf')
