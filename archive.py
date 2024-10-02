from google.cloud import storage
from google.auth import default
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

def archive_files(config, credentials, date_str):

    credentials, project = default()
    target_project = config['send']['gcp']['target_project']
    storage_client = storage.Client(credentials=credentials, project=target_project)
    # 设置你的存储桶名称
    bucket_name = config['get']['gcp']['bucket_name']
    input_folder = config['get']['gcp']['folder']
    archive_folder = f"{config['get']['gcp']['archive']}{date_str}/"
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name,prefix=input_folder, delimiter=None)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        source_name = blob.name
        if source_name.endswith('.csv') or source_name.endswith('.txt'):
            destination_blob_name = source_name.replace(input_folder,archive_folder)
            move_blob(bucket_name, source_name, bucket_name, destination_blob_name,storage_client)
        else:
            logging.info(f"skip {source_name}")
