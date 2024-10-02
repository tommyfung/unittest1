import paramiko
import io
import os
import logging
import textwrap
from google.auth import default
from google.cloud import storage
from google.cloud import secretmanager
from datetime import datetime
import socks
import fnmatch
from zip_files import zip_files
from archive import archive_files

def ensure_directory_exists(sftp, remote_directory):
    directories = remote_directory.split('/')
    path = ''
    for directory in directories:
        if directory:  # Skip empty strings from split
            path = os.path.join(path, directory)
            try:
                sftp.listdir(path)
            except IOError:
                try:
                    sftp.mkdir(path)
                    logging.info(f"INdexLab - Send SFTP directory '{path}' created successfully.")
                except Exception as e:
                    logging.error(f"INdexLab - Send SFTP failed to create directory '{path}': {e}")
                    return

def send_files(config, credential, date_str):
    hostname = config['send']['sftp']['hostname']
    port = config['send']['sftp']['port']
    username = config['send']['sftp']['username']
    remote_path = config['send']['sftp']['remote_path']
    use = config['use']
    proxy = config['send']['sftp'].get('proxy', None)

    sent_files = []
    not_found_files = []

    # Timestamp for logging
    timestamp = datetime.utcnow().isoformat()
    logging.info(f"INdexLab - Send - Timestamp: {timestamp}")

    try:
        if use == 'gcp':
            # GCP storage client
            files = config['send']['gcp']['files']
            credentials, project = default()
            target_project = config['send']['gcp']['target_project']
            storage_client = storage.Client(credentials=credentials, project=target_project)
            bucket_name = config['send']['gcp']['bucket_name']
            folder = config['send']['gcp']['folder']
            bucket = storage_client.bucket(bucket_name)
            exception_list = config['send']['gcp']['exception_list']
            secret_path = config['send']['gcp']['secret_path']
            
            # Create the Secret Manager client.
            client = secretmanager.SecretManagerServiceClient()
            logging.info("INdexLab - Send - SecretManagerServiceClient")
            # Access the secret version.
            response = client.access_secret_version(request={"name": secret_path})
            logging.info(f"INdexLab - Send - response : {response}")
            # Retrieve the private key.
            private_key = response.payload.data.decode("UTF-8")
            logging.info(f"INdexLab - Send - private_key 1 : {private_key}")
            private_key = textwrap.dedent(private_key).strip()
            logging.info(f"INdexLab - Send - private_key 2 : {private_key}")
            private_key = private_key.replace(" ", "")
            private_key = private_key.replace("\n", "")
            private_key = private_key.replace("-----BEGINRSAPRIVATEKEY-----", "-----BEGIN RSA PRIVATE KEY-----\n")
            private_key = private_key.replace("-----ENDRSAPRIVATEKEY-----", "\n-----END RSA PRIVATE KEY-----")
            logging.info(f"INdexLab - Send - private_key 3 : {private_key}")
            private_key_file = io.StringIO(private_key)
            # Load the private key.
            pkey = paramiko.RSAKey.from_private_key(private_key_file)
            #logging.info(f"INdexLab - Send - pkey : {pkey}")
            
            # Connect to the SFTP server
            if proxy:
                proxy_host = proxy['host']
                proxy_port = proxy['port']
                proxy_type = socks.HTTP

                # Create a socks socket
                sock = socks.socksocket()
                sock.set_proxy(proxy_type, proxy_host, proxy_port)
                sock.connect((hostname, port))
                logging.info(f"INdexLab - Send - Connected to proxy {proxy_host}:{proxy_port}")

                transport = paramiko.Transport(sock)
                transport.connect(username=username, pkey=pkey)                              
                logging.info("SFTP connection established successfully through proxy.")

            sftp = paramiko.SFTPClient.from_transport(transport)
            
            for file_pattern in files:
                # Replace placeholders with actual values
                if 'date_str' in file_pattern:
                    file_pattern = file_pattern.replace('date_str', date_str)
                    #logging.info(f"INdexLab - Send - Renamed file pattern: {file_pattern}")
                if 'yyyyMM' in file_pattern:
                    file_pattern = file_pattern.replace('yyyyMM', date_str[:6])
                    #logging.info(f"INdexLab - Send - Renamed file pattern: {file_pattern}")

                logging.info(f"INdexLab - Send - Processing pattern: {file_pattern}")

                # List all blobs in the bucket
                blobs = bucket.list_blobs(prefix=folder)
                matched = False
                for blob in blobs:
                    #logging.info(f"INdexLab - Send - Blob: {blob.name}")
                    blob.name = blob.name[len(folder):]
                    #logging.info(f"INdexLab - Send - new Bloc: {blob.name}")
                    if fnmatch.fnmatch(blob.name, file_pattern):
                        matched = True
                        file_name = blob.name
                        #logging.info(f"INdexLab - exception_list: {exception_list} - file_name: {file_name}")
                        if any(exc in file_name for exc in exception_list):
                            logging.info(f"INdexLab - Send - File '{file_name}' skipped due to exception list.")
                            continue
                        try:
                            file_obj = io.BytesIO()
                            blob.download_to_file(file_obj)
                            file_obj.seek(0)                            

                            # Upload the file to the SFTP server                            
                            remote_file_path = os.path.join(remote_path, file_name)  # Revised to maintain file structure
                            remote_directory = os.path.dirname(remote_file_path)
                            #logging.info(f"INdexLab - Send - SFTP DEBUG ### 0 'remote: {remote_directory}' remote_file_path: {remote_file_path} ")
                            ensure_directory_exists(sftp, remote_directory)
                            sftp.putfo(file_obj, remote_file_path)
                            logging.info(f"INdexLab - Send - File '{file_name}' uploaded to SFTP server '{hostname}:{port}'")

                            sent_files.append(file_name)
                        except Exception as e:
                            logging.warning(f"INdexLab - Send - Error uploading file '{file_name}': {e}")
                            not_found_files.append(file_name)
                if not matched:
                    logging.info(f"INdexLab - Send - No files matched for pattern: {file_pattern}")

            sftp.close()
            transport.close()

    except paramiko.SSHException as e:
        logging.error(f"INdexLab - Send - SSH error: {e}")
    except paramiko.AuthenticationException as e:
        logging.error(f"INdexLab - Send - Authentication error: {e}")
    except paramiko.SFTPError as e:
        logging.error(f"INdexLab - Send - SFTP error: {e}")
    except Exception as e:
        logging.error(f"INdexLab - Send - Unexpected error: {e}")

    return {
        "message": "Files processed.",
        "sent_files": sent_files,
        "not_found_files": not_found_files
    }
