from __future__ import print_function

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import email_operator

# See https://airflow.apache.org/integration.html#cloud-storage for help using Google Cloud Storage operators
# See https://cloud.google.com/composer/docs/how-to/using/writing-dags#gcp_name_operators for help using other Google platform operators
from airflow.contrib.operators import gcs_download_operator
# E.g. gcs_download_operator.GoogleCloudStorageDownloadOperator(bucket, object, filename=False, store_to_xcom_key=False, google_cloud_storage_conn_id='google_cloud_storage_default', delegate_to=None, *args, **kwargs)
from airflow.contrib.operators import gcs_to_bq
# E.g. gcs_to_bq.GoogleCloudStorageToBigQueryOperator(bucket, source_objects, destination_project_dataset_table, schema_fields=None, schema_object=None, source_format='CSV', create_disposition='CREATE_IF_NEEDED', skip_leading_rows=0, write_disposition='WRITE_EMPTY', field_delimiter=', ', max_bad_records=0, quote_character=None, allow_quoted_newlines=False, allow_jagged_rows=False, max_id_key=None, bigquery_conn_id='bigquery_default', google_cloud_storage_conn_id='google_cloud_storage_default', delegate_to=None, schema_update_options=(), src_fmt_configs={}, *args, **kwargs)
from airflow.contrib.ohooks import gcs_hook
# See https://airflow.apache.org/integration.html#googlecloudstoragehook for information on how to use this hook
# E.g. gcs_hook.GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_storage_default', delegate_to=None)

default_dag_args = {
	'owner': 'friedemann',
	'retries': 2,
	'retry_delay': timedelta(minutes=20),
	'start_date': datetime(2018, 7, 31),
	'end_date': None,
	'depends_on_past': False, #This decides whether or not this task depends on prior tasks' successful completion to be allowed to run
	'email': ['friedemann.ang@gmail.com'],
	'email_on_failure': True,
	'email_on_retry': False,
}

with models.DAG(
	'jpstat_ingest',
	schedule_interval=datetime.timedelta(days=7),
	default_args=default_dag_args
	) as dag:

	# Define functions here if necessary

	###

	# Define operators here too, e.g. 
    hello_python = python_operator.PythonOperator(
	    task_id='hello',
	    python_callable=greeting)

    goodbye_bash = bash_operator.BashOperator(
        task_id='bye',
        bash_command='echo Goodbye.')

    # Finally, define the order in which tasks complete by using the >> and << operators, e.g.
    hello_python >> goodbye_bash