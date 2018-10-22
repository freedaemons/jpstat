from __future__ import print_function

import airflow

from airflow import models
from airflow.operators import dummy_operator
from airflow.operators import subdag_operator

from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators import email_operator

from datetime import datetime, timedelta

# See https://airflow.apache.org/integration.html#cloud-storage for help using Google Cloud Storage operators
# See https://cloud.google.com/composer/docs/how-to/using/writing-dags#gcp_name_operators for help using other Google platform operators
from airflow.contrib.operators import gcs_download_operator
# E.g. gcs_download_operator.GoogleCloudStorageDownloadOperator(bucket, object, filename=False, store_to_xcom_key=False, google_cloud_storage_conn_id='google_cloud_storage_default', delegate_to=None, *args, **kwargs)
from airflow.contrib.operators import gcs_to_bq
# E.g. gcs_to_bq.GoogleCloudStorageToBigQueryOperator(bucket, source_objects, destination_project_dataset_table, schema_fields=None, schema_object=None, source_format='CSV', create_disposition='CREATE_IF_NEEDED', skip_leading_rows=0, write_disposition='WRITE_EMPTY', field_delimiter=', ', max_bad_records=0, quote_character=None, allow_quoted_newlines=False, allow_jagged_rows=False, max_id_key=None, bigquery_conn_id='bigquery_default', google_cloud_storage_conn_id='google_cloud_storage_default', delegate_to=None, schema_update_options=(), src_fmt_configs={}, *args, **kwargs)
from airflow.contrib.ohooks import gcs_hook
# See https://airflow.apache.org/integration.html#googlecloudstoragehook for information on how to use this hook
# E.g. gcs_hook.GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_storage_default', delegate_to=None)

import sys
sys.path.append(os.path.join(os.pardir, 'tasks'))
import ingest.files_review_task
import ingest.files_downupload_task

# We can also structure DAGs and tasks like this:
transform_dag = models.DAG(
	dag_id='jpstat_transform',
	schedule_interval=timedelta(days=7),
	catchup=False,
	default_args=default_dag_args
	)

transform_start = dummy_operator.DummyOperator(
	task_id='transform-start',
	default_args=default_dag_args,
	dag=transform_dag
	)

section_1 = subdag_operator.SubDagOperator(
	task_id='section-1',
	subdag=subdag('transform_subdag_1', 'section-1', args),
	default_args=default_dag_args,
	dag=transform_dag
	)

section_2 = subdag_operator.SubDagOperator(
	task_id='section-1',
	subdag=subdag('transform_subdag_2', 'section-1', args),
	default_args=default_dag_args,
	dag=transform_dag
	)

transform_end = dummy_operator.DummyOperator(
	task_id='transform-end',
	default_args=default_dag_args,
	dag=transform_dag
	)

transform_start.set_downstream(section_1)
section_1.set_downstream(section_2)
section_2.set_downstream(transform_end)