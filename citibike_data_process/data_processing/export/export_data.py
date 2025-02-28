import os
import logging

from dotenv import load_dotenv
from ...shared_util.s3_functions import upload_to_s3

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "../../.."))

LOCAL_DB_PATH = os.path.join(PROJECT_DIR, os.getenv("DUCKDB_FILE"))

S3_BUCKET = "ghn-public-data"
S3_FILE_PATH = f'citibike-data/{os.getenv("DUCKDB_FILE")}'


def export_data(conn, args):
    if args.make_remote:
        conn.close()
        upload_to_s3(LOCAL_DB_PATH, S3_BUCKET, S3_FILE_PATH)
        logging.info(
            f"✅ File successfully uploaded to s3://{S3_BUCKET}/{S3_FILE_PATH}")
    if args.read_remote and args.make_remote:
        os.remove(LOCAL_DB_PATH)
