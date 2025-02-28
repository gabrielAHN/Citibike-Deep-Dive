import os
import duckdb
import argparse
import logging
import time

from dotenv import load_dotenv

from .data_processing.ingestion.ingestion import retrieve_data
from .data_processing.export.export_data import export_data
from .data_processing.setup.table_list import table_list
from .data_processing.setup.db_setup import set_up_db
from .shared_util.s3_functions import download_file
from .data_processing.ingestion.db_importing import db_import

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    start_time = time.time()
    logging.info("Starting the data processing pipeline.")
    
    parser = argparse.ArgumentParser()
    
    read_group = parser.add_mutually_exclusive_group(required=True)
    read_group.add_argument('--read-remote', action='store_true', help='Read data from the remote source.')
    read_group.add_argument('--read-local', action='store_true', help='Read data from the local source.')
    
    make_group = parser.add_mutually_exclusive_group(required=True)
    make_group.add_argument('--make-local', action='store_true', help='Process and store data locally.')
    make_group.add_argument('--make-remote', action='store_true', help='Process and upload data to a remote bucket.')
    
    file_types = parser.add_mutually_exclusive_group(required=True)
    file_types.add_argument('--file-local', action='store_true', help='Process and store data locally.')
    file_types.add_argument('--file-remote', action='store_true', help='Process and upload data to a remote bucket.')
    
    parser.add_argument('--threads', type=int, default=5, help='Number of threads for multi-threading (default: 5).')
    
    args = parser.parse_args()
    
    if args.read_remote:
        download_file(
            bucket='ghn-public-data',
            file_name=os.getenv('DUCKDB_FILE'),
            file_date=f"citibike-data/{os.getenv('DUCKDB_FILE')}"
        )
        logging.info("Retrieved 🔗 DuckDB Remote File 💾.")
        

    logging.info("Connecting to local DuckDB database.")
    conn = duckdb.connect(database=os.getenv('DUCKDB_FILE'), read_only=False)
    
    logging.info("Setting up the database.")
    set_up_db(conn, args)
    
    new_files = retrieve_data(conn=conn, args=args, workers=args.threads)
    
    if new_files:
        logging.info("New data files retrieved. Processing tables.")
        for new_file in new_files:
            logging.info(f"⚙️ Processing File: {new_file.table_name}")
            db_import(conn, new_file)
            for table in table_list:
                logging.info(f"Creating {table.name} for {new_file.table_name}")
                table.create_table(conn)
                table.update_function(
                    conn=conn,
                    name=table.name,
                    new_files=[new_file],
                    workers=args.threads
                ) 
                logging.info(f"✅ Updated {table.name} for {new_file.table_name}")
        logging.info("Exporting processed data.")
        export_data(conn, args)
    else:
        logging.info("No new data available.")
    
    end_time = time.time()
    logging.info(f"Data processing pipeline completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
