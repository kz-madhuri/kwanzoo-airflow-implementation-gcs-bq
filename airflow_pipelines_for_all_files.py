from google.cloud import bigquery
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.api_core import page_iterator
from google.cloud import storage
from airflow.models import Variable

def _item_to_value(iterator, item):
    return item

def list_directories(bucket_name, prefix):
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    extra_params = {
        "projection": "noAcl",
        "prefix": prefix,
        "delimiter": '/'
    }

    gcs = storage.Client()

    path = "/b/" + bucket_name + "/o"

    iterator = page_iterator.HTTPIterator(
        client=gcs,
        api_request=gcs._connection.api_request,
        path=path,
        items_key='prefixes',
        item_to_value=_item_to_value,
        extra_params=extra_params,
    )

    return [x for x in iterator]

def process_latest_folder(bucket_name, folder_name, filename):
    folders = list_directories(bucket_name, folder_name)
    file_folders = [folder_path for folder_path in folders if str(filename) in folder_path]
    file_folders.sort(reverse=True)
    latest_folder_path = file_folders[0]
    latest_file_uri = f'gs://{bucket_name}/{latest_folder_path}'
    return latest_file_uri

def check_latest_version(latest_file_uri):
    latest_file_path = latest_file_uri
    prev_version_b2b = Variable.get("current_latest_version_b2b")
    filename_full = latest_file_path.split('/')[-2]
    filename, version_str = filename_full.split('_', 1)
    if version_str == prev_version_b2b:
        print("Load should be stopped")
        return 1
    else:
        print("Load should be triggered")
        Variable.set("current_latest_version_b2b",version_str)
        print("Current latest version also updated")
        return 0

def skip_downstream_task():
    print("SQL Downstream task skipped because of version similarities")




with DAG('b2b_pipeline_dag', start_date=datetime(2023, 8, 1), schedule_interval='30 6 * * Mon') as dag:
    
    bucket_name = Variable.set("bucket_name", 'trovo-transfer')
    folder_name = Variable.set("folder_name", 's3-gcs-sample-transfer')
    filename = Variable.set("filename", 'b2bexport')
    current_latest_version_b2b = Variable.set("current_latest_version_b2b", '5_2_1')
    
    folder_task = PythonOperator(
        task_id='process_latest_folder',
        python_callable=process_latest_folder,
        op_kwargs={'bucket_name': Variable.get("bucket_name"),
                   'folder_name': Variable.get("folder_name"),
                   'filename': Variable.get("filename")},
        provide_context=True,
    )
    
    check_task = PythonOperator(
        task_id='check_latest_version',
        python_callable=check_latest_version,
        op_kwargs={'latest_file_uri':"{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        provide_context=True,
    )
    
    sql_task = BigQueryExecuteQueryOperator(
        task_id='load_b2b_to_bq_sql',
        params={'latest_gcs_uri': "{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        sql='load_b2b_to_bq.sql',
        use_legacy_sql=False,
    )
    
    short_circuit_task = ShortCircuitOperator(
        task_id='short_circuit_task',
        python_callable=skip_downstream_task,
    )
    
    folder_task >> check_task >> [sql_task, short_circuit_task]

with DAG('b2c_pipeline_dag', start_date=datetime(2023, 8, 1), schedule_interval='30 6 * * Mon') as dag:
    
    bucket_name = Variable.set("bucket_name", 'trovo-transfer')
    folder_name = Variable.set("folder_name", 's3-gcs-sample-transfer')
    filename = Variable.set("filename", 'b2c')
    current_latest_version_b2b = Variable.set("current_latest_version_b2c", '2_0_0')
    
    folder_task = PythonOperator(
        task_id='process_latest_folder',
        python_callable=process_latest_folder,
        op_kwargs={'bucket_name': Variable.get("bucket_name"),
                   'folder_name': Variable.get("folder_name"),
                   'filename': Variable.get("filename")},
        provide_context=True,
    )
    
    check_task = PythonOperator(
        task_id='check_latest_version',
        python_callable=check_latest_version,
        op_kwargs={'latest_file_uri':"{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        provide_context=True,
    )
    
    sql_task = BigQueryExecuteQueryOperator(
        task_id='load_b2c_to_bq_sql',
        params={'latest_gcs_uri': "{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        sql='load_b2c_to_bq.sql',
        use_legacy_sql=False,
    )
    
    short_circuit_task = ShortCircuitOperator(
        task_id='short_circuit_task',
        python_callable=skip_downstream_task,
    )
    
    folder_task >> check_task >> [sql_task, short_circuit_task]

with DAG('firmographic_pipeline_dag', start_date=datetime(2023, 8, 1), schedule_interval='30 6 * * Mon') as dag:
    
    bucket_name = Variable.set("bucket_name", 'trovo-transfer')
    folder_name = Variable.set("folder_name", 's3-gcs-sample-transfer')
    filename = Variable.set("filename", 'firmographic')
    current_latest_version_b2b = Variable.set("current_latest_version_firmo", '1_8_0')
    
    folder_task = PythonOperator(
        task_id='process_latest_folder',
        python_callable=process_latest_folder,
        op_kwargs={'bucket_name': Variable.get("bucket_name"),
                   'folder_name': Variable.get("folder_name"),
                   'filename': Variable.get("filename")},
        provide_context=True,
    )
    
    check_task = PythonOperator(
        task_id='check_latest_version',
        python_callable=check_latest_version,
        op_kwargs={'latest_file_uri':"{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        provide_context=True,
    )
    
    sql_task = BigQueryExecuteQueryOperator(
        task_id='load_firmographic_to_bq_sql',
        params={'latest_gcs_uri': "{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        sql='load_firmographic_to_bq.sql',
        use_legacy_sql=False,
    )
    
    short_circuit_task = ShortCircuitOperator(
        task_id='short_circuit_task',
        python_callable=skip_downstream_task,
    )
    
    folder_task >> check_task >> [sql_task, short_circuit_task]

with DAG('ip2company_pipeline_dag', start_date=datetime(2023, 8, 1), schedule_interval='30 6 * * Mon') as dag:
    
    bucket_name = Variable.set("bucket_name", 'trovo-transfer')
    folder_name = Variable.set("folder_name", 's3-gcs-sample-transfer')
    filename = Variable.set("filename", 'ip_to_company')
    current_latest_version_b2b = Variable.set("current_latest_version_ip2comp", '2_11_0')
    
    folder_task = PythonOperator(
        task_id='process_latest_folder',
        python_callable=process_latest_folder,
        op_kwargs={'bucket_name': Variable.get("bucket_name"),
                   'folder_name': Variable.get("folder_name"),
                   'filename': Variable.get("filename")},
        provide_context=True,
    )
    
    check_task = PythonOperator(
        task_id='check_latest_version',
        python_callable=check_latest_version,
        op_kwargs={'latest_file_uri':"{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        provide_context=True,
    )
    
    sql_task = BigQueryExecuteQueryOperator(
        task_id='load_firmographic_to_bq_sql',
        params={'latest_gcs_uri': "{{ task_instance.xcom_pull(task_ids='process_latest_folder') }}"},
        sql='load_firmographic_to_bq.sql',
        use_legacy_sql=False,
    )
    
    short_circuit_task = ShortCircuitOperator(
        task_id='short_circuit_task',
        python_callable=skip_downstream_task,
    )
    
    folder_task >> check_task >> [sql_task, short_circuit_task]