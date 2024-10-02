import paramiko
import io
import os
import logging
import textwrap
from azure.storage.blob import BlobServiceClient
from google.auth import default
from google.cloud import storage
from google.cloud import secretmanager
from datetime import datetime
import socks
import fnmatch

def get_files(config, credential, date_str):
    hostname = config['get']['sftp']['hostname']
    port = config['get']['sftp']['port']
    username = config['get']['sftp']['username']
    files = config['get']['sftp']['files']
    remote_path = config['get']['sftp']['remote_path']
    use = config['use']
    proxy = config['get']['sftp'].get('proxy', None)
    secret_path = config['get']['gcp']['secret_path']

    downloaded_files = []
    not_found_files = []

    # Timestamp for logging
    timestamp = datetime.utcnow().isoformat()
    logging.info(f"INdexLab - Get - Timestamp: {timestamp}")

    try:
        # Construct the SFTP command
        if proxy:
            proxy_host = proxy['host']
            proxy_port = proxy['port']
            proxy_type = socks.HTTP

            # Create a socks socket
            sock = socks.socksocket()
            sock.set_proxy(proxy_type, proxy_host, proxy_port)
            sock.connect((hostname, port))

            # Create the Secret Manager client.
            client = secretmanager.SecretManagerServiceClient()
            # Access the secret version.
            response = client.access_secret_version(request={"name": secret_path})
            # Retrieve the private key.
            private_key = response.payload.data.decode("UTF-8")
            private_key = textwrap.dedent(private_key).strip()
            private_key = private_key.replace(" ", "")
            private_key = private_key.replace("\n", "")
            private_key = private_key.replace("-----BEGINRSAPRIVATEKEY-----", "-----BEGIN RSA PRIVATE KEY-----\n")
            private_key = private_key.replace("-----ENDRSAPRIVATEKEY-----", "\n-----END RSA PRIVATE KEY-----")
            private_key_file = io.StringIO(private_key)
            # Load the private key.
            pkey = paramiko.RSAKey.from_private_key(private_key_file)

            transport = paramiko.Transport(sock)
            transport.connect(username=username, pkey=pkey)
            logging.info(f"INdexLab - Get - Connected to proxy {proxy_host}:{proxy_port}")
            logging.info("SFTP connection established successfully.")

        sftp = paramiko.SFTPClient.from_transport(transport)

        def process_files(remote_dir):
           all_files_processed = True  # Flag to track if all files are processed
           for file_name in sftp.listdir(remote_dir):
               remote_file_path = os.path.join(remote_dir, file_name)
               if sftp.stat(remote_file_path).st_mode & 0o170000 == 0o040000:  # Check if it's a directory
                   process_files(remote_file_path)
               else:
                   file_matched = False  # Flag to track if the current file matches any pattern
                   for file_pattern in files:
                       file_pattern = file_pattern.replace('yyyyMM', date_str[:6]).replace('yyyy', date_str[:4])
                       if fnmatch.fnmatch(remote_file_path, os.path.join(remote_path, file_pattern)):
                           file_obj = io.BytesIO()
                           sftp.getfo(remote_file_path, file_obj)
                           file_obj.seek(0)
                           if use == 'gcp':
                               # Remove the root directory from the path
                               relative_path = os.path.relpath(remote_file_path, remote_path)
                               # Upload the file to GCP Bucket
                               blob_path = os.path.join(folder, relative_path)
                               blob = bucket.blob(blob_path)
                               blob.upload_from_file(file_obj)
                               logging.info(f"INdexLab - Get - File '{relative_path}' uploaded to GCP bucket '{bucket_name}/{folder}'")
                           downloaded_files.append(remote_file_path)
                           file_matched = True
                           break
                   if not file_matched:
                       logging.error(f"File '{remote_file_path}' does not match any pattern in the list.")
                       all_files_processed = False  # Set flag to False if any file does not match
           if not all_files_processed:
               raise Exception("Some files did not match any pattern. Program terminated.")

        if use == 'gcp':
           credentials, project = default()
           target_project = config['get']['gcp']['target_project']
           storage_client = storage.Client(credentials=credentials, project=target_project)
           bucket_name = config['get']['gcp']['bucket_name']
           folder = config['get']['gcp']['folder']
           bucket = storage_client.bucket(bucket_name)
           
        process_files(remote_path)
        sftp.close()
        transport.close()
        
        if all_files_processed:
           logging.info("All files processed successfully.")
           zip_files(config, date_str)
           archive_files(config, credentials, date_str)

    except paramiko.SSHException as e:
        logging.error(f"INdexLab - Get - SSH error: {e}")
    except paramiko.AuthenticationException as e:
        logging.error(f"INdexLab - Get - Authentication error: {e}")
    except paramiko.SFTPError as e:
        logging.error(f"INdexLab - Get - SFTP error: {e}")
    except Exception as e:
        logging.error(f"INdexLab - Get - Unexpected error: {e}")

    return {
        "message": "Files processed.",
        "downloaded_and zipped_files": downloaded_files,
        "not_found_files": not_found_files
    }
