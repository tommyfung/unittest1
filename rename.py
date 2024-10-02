from google.cloud import storage
from google.auth import default
import re
import os
import logging

def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name, storage_client):

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    destination_generation_match_precondition = 0

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
    )
    source_bucket.delete_blob(blob_name)

def rename_files(config, credentials):

    credentials, project = default()
    target_project = config['send']['gcp']['target_project']
    storage_client = storage.Client(credentials=credentials, project=target_project)
    # 设置你的存储桶名称
    bucket_name = config['get']['gcp']['bucket_name']
    input_folder = config['get']['gcp']['folder']
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name,prefix=input_folder, delimiter=None)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        source_name = blob.name
        if source_name.endswith('000000000000.csv'):
            destination_blob_name = re.sub(r'.0+.csv', '.csv', source_name)
            move_blob(bucket_name, source_name, bucket_name, destination_blob_name,storage_client)
        else:
            logging.info(f"skip {source_name}")
