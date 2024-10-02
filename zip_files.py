import zipfile
import io
import logging
import fnmatch
from google.auth import default
from google.cloud import storage
from datetime import datetime

def zip_files(config, date_str):
    source_bucket_name = config['zip']['source_bucket']
    destination_bucket_name = config['zip']['destination_bucket']
    files = config['zip']['files']
    output_folder = config['zip']['output_folder']
    target_project = config['zip']['target_project']
    folder = config['zip']['folder']
    option = config['zip'].get('option', None)

    # Extract yyyyMM from date_str
    yyyyMM = date_str[:6]
    yyyy = date_str[:4]

    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Initialize GCP storage client
    credentials, project = default()
    storage_client = storage.Client(credentials=credentials, project=target_project)
    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    if option == "notzip":
        # Directly copy files from source to destination bucket
        files_copied = []
        blobs = source_bucket.list_blobs(prefix=folder)
        for blob in blobs:
            blob_name = blob.name[len(folder):]
            for file_pattern in files:
                # Replace placeholders with actual values
                if 'date_str' in file_pattern:
                    file_pattern = file_pattern.replace('date_str', date_str)
                file_pattern = file_pattern.replace('yyyyMM', yyyyMM).replace('yyyy', yyyy)
                if fnmatch.fnmatch(blob_name, file_pattern):
                    try:
                        destination_blob = destination_bucket.blob(f"{output_folder}/{blob_name}")
                        destination_blob.rewrite(blob)
                        files_copied.append(blob_name)
                        logging.info(f"INdexLab - notzip - Successfully copied '{blob_name}' to destination bucket.")
                    except Exception as e:
                        logging.warning(f"INdexLab - notzip - Error copying file '{blob_name}': {e}")
                    break  # Stop checking other patterns once a match is found
        return {
            "message": "Files copied to destination bucket.",
            "files_copied": files_copied
        }

    else:
        # Create a zip file in memory
        zip_buffer = io.BytesIO()
        files_zipped = []
        blobs = source_bucket.list_blobs(prefix=folder)
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for blob in blobs:
                blob_name = blob.name[len(folder):]
                for file_pattern in files:
                    # Replace placeholders with actual values
                    if 'date_str' in file_pattern:
                        file_pattern = file_pattern.replace('date_str', date_str)
                    file_pattern = file_pattern.replace('yyyyMM', yyyyMM).replace('yyyy', yyyy)
                    if fnmatch.fnmatch(blob_name, file_pattern):
                        try:
                            file_obj = io.BytesIO()
                            blob.download_to_file(file_obj)
                            file_obj.seek(0)
                            zip_file.writestr(blob_name, file_obj.read())
                            files_zipped.append(blob_name)
                            logging.info(f"INdexLab - Zip - Successfully added '{blob_name}' to zip.")
                        except Exception as e:
                            logging.warning(f"INdexLab - Zip - Error adding file '{blob_name}' to zip: {e}")
                        break  # Stop checking other patterns once a match is found
        zip_buffer.seek(0)
        # Upload the zip file to the destination bucket
        zip_filename = f"{output_folder}/INdexLab_{date_str}.zip"
        blob = destination_bucket.blob(zip_filename)
        blob.upload_from_file(zip_buffer, content_type='application/zip')
        logging.info(f"INdexLab - Zip - Zip file '{zip_filename}' uploaded to GCP bucket '{destination_bucket_name}'")
        logging.info(f"INdexLab - Zip - Files zipped: {files_zipped}")
        return {
            "message": "Files zipped and uploaded.",
            "zip_filename": zip_filename,
            "files_zipped": files_zipped
        }
