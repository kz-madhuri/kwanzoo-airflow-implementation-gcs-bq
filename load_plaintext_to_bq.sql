LOAD DATA OVERWRITE `kwanzoo-july-2022.5x5_staging.plaintext_table_master`
FROM FILES (
  format = 'CSV',
  uris = ['gs://trovo-transfer/s3-gcs-sample-transfer/plaintext_email/*.csv.gz']);