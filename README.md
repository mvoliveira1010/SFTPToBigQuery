# SFTPToBigQuery
ELT batch pipeline to get files from SFTP and upload them to Cloud Storage, then open from Cloud Storage and ingesting data to BigQuery.


**1º step:** fn-sftp-to-gcs.py

**Description:** This Python script is triggered by HTTP request, open a SSH and SFTP connection, get the file by file_name param, and write it to a blob on a Cloud Storage bucket).

**2º step:** fn-gcs-to-bqs.py

**Description:** This Python script is triggered by Cloud Storage blob creation action, open the blob by blob_name param, as a dataframe, then ingest dataframe to BigQuery table.
![Team document](https://github.com/mvoliveira1010/SFTPToBigQuery/assets/67582983/091a9cf1-978f-4dd0-a45f-8cf4072129cc)

**Set up the DATA LIFECYCLE RULE to delete the blobs on Cloud Storage after x days.**
