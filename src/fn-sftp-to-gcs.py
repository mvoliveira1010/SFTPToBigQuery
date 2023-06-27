import logging
import os
from datetime import datetime
import xml.etree.ElementTree as ET
import requests
from google.cloud import storage
import paramiko

logger = logging.getLogger()
time = datetime.now()
bucket_name = os.environ['BUCKET']

def main(request):
    request_json = request.get_json()
    file_name = request_json['file_table']
    remote_file_path = f"/Hml_Sabro/{file_name}"
    print(remote_file_path)
    extract_from_server(file_name, remote_file_path)
    return 'Finished!'

def extract_from_server(file_name, remote_file_path):
    consumer_key = os.environ['CONSUMER_KEY']
    oauth_token = os.environ['OAUTH_TOKEN']
    host = os.environ['SERVIDOR']
    port = os.environ['PORTA']
    username = os.environ['USUARIO']
    password = os.environ['SENHA']
    print('Credenciais OK!')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, port, username, password)
    sftp_client = ssh.open_sftp()
    print('Conectou ao sftp')
    with sftp_client.open(remote_file_path) as arquivo:
        upload_to_gcs(bucket_name, file_name, arquivo)
    print(f"Upload do arquivo {file_name} concluído!")
    sftp_client.close()
    ssh.close()

def upload_to_gcs(bucket_name, file_name, arquivo):
    folder_name = time.strftime("%Y-%m-%d")
    blob_name = f"{folder_name}/{file_name}"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(arquivo)
