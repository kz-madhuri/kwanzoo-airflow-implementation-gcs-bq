CREATE OR REPLACE TABLE `kwanzoo-july-2022.5x5_staging_archives.b2bexport_data_copy`
COPY `kwanzoo-july-2022.5x5_staging.b2b_master`;

LOAD DATA OVERWRITE `kwanzoo-july-2022.5x5_staging.b2b_master`
FROM FILES (
  format = 'CSV',
  uris = [@latest_gcs_uri]);