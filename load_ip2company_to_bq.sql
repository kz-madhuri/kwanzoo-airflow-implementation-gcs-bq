CREATE OR REPLACE TABLE `kwanzoo-july-2022.5x5staging_archives.ip2comp_data_copy`
COPY `kwanzoo-july-2022.5x5_staging.iptocompany_master`;

LOAD DATA OVERWRITE `kwanzoo-july-2022.5x5_staging.iptocompany_master`
FROM FILES (
  format = 'CSV',
  uris = [@latest_gcs_uri]);
  
