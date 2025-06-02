from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from datetime import datetime, timedelta
from scripts.upload_to_s3 import download_upload_task
from dotenv import load_dotenv
import os

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
    
    # Create a temporary S3 bucket
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
    
    # Delete the S3 bucket (cleanup)
    # delete_bucket = S3DeleteBucketOperator(
    #     task_id='s3_bucket_dag_delete',
    #     bucket_name=UNIQUE_BUCKET_NAME,
    #     force_delete=True,
    #     aws_conn_id='aws_default',
    # )
    
    # Define task dependencies
    # create_bucket >> download_and_upload_data >> delete_bucket
    create_bucket >> download_and_upload_data