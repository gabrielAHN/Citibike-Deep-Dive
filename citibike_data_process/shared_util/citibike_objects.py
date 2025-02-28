from .parser import create_table_name

from ..data_processing.ingestion.zip_processing import (
    process_zip_file, combine_zip_datasets
)


class FileObject:
    def __init__(self, file, name, workers):
        self.file = combine_zip_datasets(
            process_zip_file(file), workers
        )
        self.table_name = self.table_name(name)

    @staticmethod
    def table_name(name):
        return create_table_name(
            table_name="Citibike",
            filename=name
        )


class TableObject:
    def __init__(self, name, sql_query, update_function):
        self.name = name
        self.sql_query = sql_query
        self.update_function = update_function

    def create_table(self, conn):
        if self.sql_query:
            conn.execute(self.sql_query)
