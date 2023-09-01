from google.cloud import bigquery
from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models import Variable

start_date = Variable.set("start_date", val='2023-08-01')
end_date = Variable.set("end_date", val='2023-08-15')


def loading_data_from_gcs_custom_date_range(start_date, end_date, **kwargs):
    client = bigquery.Client()
    table_id = "kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_unexploded"
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
    current_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    final_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    while current_date <= final_date:
        try:
            uri = f'gs://trovo-transfer/s3-gcs-sample-transfer/cookie_sync/hem_bito/y={current_date.year}/m={current_date.month}/d={current_date.day}/h=6/*.snappy.parquet'
            load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result()
            destination_table = client.get_table(table_id)
            print("Loaded {} rows.".format(destination_table.num_rows))
            current_date += timedelta(days=1)
        except Exception:
            print("Exception handled, skipping to next date")
            current_date += timedelta(days=1)
            continue


def update_start_end_date(start_date,end_date,**kwargs):
    new_start_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    new_end_date = (datetime.strptime(new_start_date, "%Y-%m-%d") + timedelta(days=15)).strftime("%Y-%m-%d")
    Variable.set("start_date", val=new_start_date)
    Variable.set("end_date", val=new_end_date)
    print("Start date {} and End date {} are updated for next run".format(new_start_date,new_end_date))


with DAG('bitos_pipeline_dag', start_date=datetime(2023, 8, 1), schedule_interval='30 6 * * Mon') as dag:
    truncate_task = BigQueryExecuteQueryOperator(
        task_id='truncate_table',
        sql="TRUNCATE TABLE `kwanzoo-july-2022.5x5_staging_intertables.hem_bito_inc_staging_unexploded`",
        use_legacy_sql=False,
    )
    
    load_task = PythonOperator(
        task_id='loading_data_from_gcs_custom_date_range',
        python_callable=loading_data_from_gcs_custom_date_range,
        op_kwargs={'start_date': Variable.get("start_date"),
                   'end_date': Variable.get("end_date")},
        provide_context=True,
    )
   
    sql_task = BigQueryExecuteQueryOperator(
        task_id='unnest_delta_tables',
        sql='load_bitos_to_bq.sql', 
        allow_large_results=True,
        use_legacy_sql=False,
    )
    
    update_task = PythonOperator(
        task_id='update_start_end_date',
        python_callable=update_start_end_date,
        op_kwargs={'start_date': Variable.get("start_date"),
                   'end_date': Variable.get("end_date")},
        provide_context=True,
    )
    
    truncate_task >> load_task >> sql_task >> update_task


