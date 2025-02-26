import io
import os

import zipfile
import pyarrow as pa
from ..setup.clean_up import read_csv_from_bytes
from ...shared_util.multi_threading import parallel_execute


def combine_zip_datasets(zip_files, workers):
    tables = parallel_execute(
        function=read_csv_from_bytes,
        data_list=zip_files,
        workers=workers
    )
    combined_table = pa.concat_tables(tables)
    return combined_table.to_pandas()


def process_zip_file(filename):
    folder_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', '..', 'data'))
    zip_path = os.path.join(folder_path, filename)
    file_data = get_zipfile_data_parallel(zip_path)
    return file_data


def get_zipfile_data_parallel(zip_path, workers=4):
    csv_buffers = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        members = zf.namelist()

    csv_members = [
        member for member in members
        if member.lower().endswith(".csv")
        and not member.startswith("__MACOSX/")
        and not member.startswith("._")
    ]
    nested_zip_members = [
        member for member in members
        if member.lower().endswith(".zip")
        and not member.startswith("__MACOSX/")
        and not member.startswith("._")
    ]

    csv_results = parallel_execute(
        lambda member: process_csv_member(zip_path, member),
        csv_members,
        workers
    )

    nested_results = parallel_execute(
        lambda member: process_nested_zip(zip_path, member),
        nested_zip_members,
        workers
    )

    csv_buffers.extend(csv_results)
    for buffers in nested_results:
        csv_buffers.extend(buffers)

    return csv_buffers


def process_csv_member(zip_path, member):
    with zipfile.ZipFile(zip_path, 'r') as zf:
        with zf.open(member) as f:
            return io.BytesIO(f.read())


def process_nested_zip(zip_path, member):
    buffers = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        with zf.open(member) as nested_zip_bytes:
            with zipfile.ZipFile(nested_zip_bytes, 'r') as child_zip:
                for child_member in child_zip.namelist():
                    if child_member.startswith("__MACOSX/") or child_member.startswith("._"):
                        continue
                    if child_member.lower().endswith(".csv"):
                        with child_zip.open(child_member) as f:
                            buffers.append(io.BytesIO(f.read()))
    return buffers
