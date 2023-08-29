CREATE OR REPLACE TABLE `kwanzoo-july-2022.5x5_staging_archives.b2cexport_data_copy`
COPY `kwanzoo-july-2022.5x5_staging.b2c_master`;

LOAD DATA OVERWRITE `kwanzoo-july-2022.5x5_staging.b2c_master`
FROM FILES (
  format = 'CSV',
  uris = [@latest_gcs_uri]);