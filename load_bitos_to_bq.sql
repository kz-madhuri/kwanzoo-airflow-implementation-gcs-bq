create or replace table `kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_exploded` as 
select distinct * from
(SELECT sha256_lower_case, bito_id FROM `kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_unexploded` cross join unnest(json_extract_array((bito_ids))) as bito_id);

create or replace table `kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_delta_backup` as
select * from `kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_exploded` as present
where not exists
(select * from `kwanzoo-july-2022.5x5_staging.hem_bito_master` as master where present.sha256_lower_case = master.SHA256_LOWER_CASE);

insert into `kwanzoo-july-2022.5x5_staging.hem_bito_master`
select * from `kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_delta_backup`;
