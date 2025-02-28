import os

from dotenv import load_dotenv

load_dotenv()

S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
S3_BUCKET_FILE = os.getenv('S3_BUCKET_FILE')
REGION = os.getenv('REGION')

def set_up_db(conn, args):
    conn.execute(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        PRAGMA threads={args.threads if args.threads else 5};
        PRAGMA memory_limit='20GB';
        PRAGMA enable_optimizer;
        """
    )
