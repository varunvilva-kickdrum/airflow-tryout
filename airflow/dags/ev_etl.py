from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from datetime import datetime, timedelta
from scripts.upload_to_s3 import download_upload_task, upload_to_s3
from dotenv import load_dotenv
import os
from scripts.spark_steps import SPARK_STEPS
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

# Load environment variables
load_dotenv()

# Generate unique bucket name ONCE at DAG definition time
UNIQUE_BUCKET_NAME = os.getenv("TEMP_BUCKET_NAME")

# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'ev_etl',
    default_args=default_args,
    description='EV ETL pipeline with temporary S3 bucket',
    schedule_interval=None,
    catchup=False,  # Don't run for past dates
    tags=['etl', 'ev-data', 's3'],
) as dag:
    
    # Create a S3 bucket
    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_dag_create',
        bucket_name=UNIQUE_BUCKET_NAME,
        aws_conn_id='aws_default',
    )
    
    # Download and upload to S3 (gets bucket name from XCom)
    download_and_upload_data = PythonOperator(
        task_id='download_and_upload_data',
        python_callable=download_upload_task,
        op_kwargs={'s3_bucket':UNIQUE_BUCKET_NAME},
        provide_context=True,
    )

    upload_spark_script = PythonOperator(
        task_id = 'upload_spark_script',
        python_callable=upload_to_s3,
        op_kwargs={
            "s3_bucket":UNIQUE_BUCKET_NAME,
            "local_file":"/home/varunvilva/Workspace/Kickdrum/airflow-workflow/airflow/dags/scripts/transform_to_hudi.py",
            "s3_key":"scripts/transform_to_hudi.py",
        },
        provide_context=True,
    )

    add_hudi_step = EmrAddStepsOperator(
        task_id="add_hudi_etl_step",
        job_flow_id="j-2MEJYD9COOY90",
        steps=SPARK_STEPS,
    )
    
    # Delete the S3 bucket (cleanup)
    # delete_bucket = S3DeleteBucketOperator(
    #     task_id='s3_bucket_dag_delete',
    #     bucket_name=UNIQUE_BUCKET_NAME,
    #     force_delete=True,
    #     aws_conn_id='aws_default',
    # )
    
    create_bucket >> download_and_upload_data >> upload_spark_script >> add_hudi_step
    