import os
import boto3
import threading

from tqdm import tqdm
from boto3.s3.transfer import TransferConfig

s3 = boto3.resource('s3')
client = boto3.client("s3")

config = TransferConfig(
    multipart_threshold=1024 * 25,
    max_concurrency=20,
    multipart_chunksize=1024 * 50,
    use_threads=True
)


def download_file(bucket, file_date, file_name):
    client.download_file(
        Bucket=bucket,
        Key=file_date,
        Filename=file_name,
        Config=config
    )


def upload_to_s3(local_path, bucket_name, s3_key):
    class ProgressPercentage:
        def __init__(self, filename):
            self._filename = filename
            self._size = float(os.path.getsize(filename))
            self._seen_so_far = 0
            self._lock = threading.Lock()
            self._pbar = tqdm(
                total=self._size,
                unit='B',
                unit_scale=True,
                desc="Uploading to S3"
            )

        def __call__(self, bytes_amount):
            with self._lock:
                self._seen_so_far += bytes_amount
                self._pbar.update(bytes_amount)

                if self._seen_so_far >= self._size:
                    self._pbar.close()

    client.upload_file(
        Filename=local_path,
        Bucket=bucket_name,
        Key=s3_key,
        Config=config,
        Callback=ProgressPercentage(local_path)
    )