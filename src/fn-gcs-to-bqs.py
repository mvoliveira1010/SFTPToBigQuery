from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from io import StringIO
import os

#Cloud Function triggered by write blobs on Cloud Storage bucket.

def main(event, context):
    file = event
    gcs_client = storage.Client()
    print(f"Processando arquivo: {file['name']}.")
    blob_name = file['name']
    bucket_name = os.environ["BUCKET_NAME"]
    df = get_gcs_blob(gcs_client, bucket_name, blob_name)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE_ID}"
    insert_from_dataframe(table_id, DF)
    return 'Finished!'

def get_gcs_blob(gcs_client, bucket_name, blob_name):
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_text()
    dataframe = pd.read_csv(StringIO(data), delimiter=';', low_memory=False)
    #Inclusion of ingestion timestamp field.
    dataframe['tmp_insert'] = pd.Timestamp.now()
    return dataframe

def insert_from_dataframe(table_id, dataframe):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )
    job.result()  # Waits for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )