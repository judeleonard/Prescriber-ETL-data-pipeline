import logging
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
import io
import glob

from datetime import datetime, timedelta
from airflow.models import DAG, Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


# config variables
creds = Variable.get("my_credential_secrets", deserialize_json=True)


def local_filesTo_s3(filepath, key, bucket_name):
    """
    functions to load files from local to s3 bucket

    Args:
        filepath: source of the files to be uploaded
        key: folder to write data to s3
        bucket_name: name of the bucket

    """
    s3 = S3Hook(aws_conn_id="aws_default")
    ingested_date = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    filename = []
    for root, dirs, files in os.walk(filepath):
        for file in files:
            if file.endswith(".csv"):
                filename.append(file)
                for file in filename:
                    file_name = os.path.join(filepath, file)
                    s3.load_file(filename=file_name, key="{}/{}".format(key, ingested_date), bucket_name=bucket_name, replace=True)
            else:
                filename.append(file)
                for file in filename:
                    file_name = os.path.join(filepath, file)
                    s3.load_file(filename=file_name, key="{}/{}".format(key, ingested_date), bucket_name=bucket_name, replace=True)
    
    print("upload to s3 successful ...")
    



def load_files_to_DB(file_name, data_path):
    """
    function that inserts all transformed data into a Postgres database

    Args:
        file_name: name of file to copy to DB
        data_path: directory pointing to file
    
    """
    engine = create_engine(str(Variable.get("DB_CONN")))
    for f in glob.glob(data_path):
       filename = f.rsplit('/')[-1]
    if filename == file_name:
        df = pd.read_csv(data_path)
        #drops old table and creates new empty table using dataframe schema
        df.head(0).to_sql('prescriber_report', engine, if_exists='replace',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        # skip any line which does not match the required field
        try:
            cur.copy_from(output, 'prescriber_report', null="") # set null values to ''
        except:
            pass
    else:
        df = pd.read_csv(data_path)
        df.head(0).to_sql('city_report', engine, if_exists='replace',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        # skip any line which does not match the required field
        try:
            cur.copy_from(output, 'city_report', null="") # set null values to ''
        except:
            pass
    if conn is not None:
        conn.commit()
        cur.close()
        conn.close()
        print("data insertion to database is completed...")


def data_quality_checks(tables):
    """
    function that checks if data was loaded to database schema

    Args:
        tables: list of tables to perform data quality checks on 
    """
    tables = tables.split(',')
    for table in tables:
        engine = create_engine(CONN_STRING, pool_size=10, max_overflow=20)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        query_records = '''SELECT COUNT(*) FROM {table}'''
        if len(query_records) < 1 or len(query_records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no result.")
        logging.info(f"Data quality on table {table} check passed with {len(query_records)} number of records")
        num_records = query_records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        logging.info(f"Data quality on table {table} check passed with {query_records[0][0]} number of records")




def local_to_blob_storage(file_path: str, prefix: str, file_name):
    """
    This function copies all the transformed data to azure blob container

    Args:
        file_path: path to file to upload to azure blob
        file_name: name of the file to be copied to azure blob
        prefix: file identifier in local staging dir
    """
    storage_account_key = creds["acct_key"]
    storage_account_name = creds["acct_name"]
    connection_string = creds["conn_str"]
    container_name = creds["container_nm"] 
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    if prefix == 'top5_presc':
        with open(file_path,"rb") as data:
            blob_client.upload_blob(data)

    else:
        with open(file_path, "rb") as data:
                blob_client.upload_blob(data)
    logging.info("{} successfully uploaded to azure blob storage".format(file_name))
  

def cleaning_process():
    """
    This functions cleans the process after pipeline has succeeded
    Input:
        None
    Output:
        None
    """
    staging_path = '/opt/airflow/staging'
    for f in glob.glob(staging_path):
        if f.rsplit('/')[-1].endswith('.csv'):
            os.remove(f)
    logging.info("cleaning process completed successfully...")


default_args = {
    'owner': 'judeleonard',
    'retries': 1,
    #'start_date': days_ago(5),
    'start_date': datetime.now(),
    'retry_delay': timedelta(seconds=3)
}

with DAG('prescriber_ingestion_pipeline',
         schedule_interval='@weekly',
         default_args = default_args,
         description ='Prescriber data ingestion pipeline',
         catchup=False) as dag:
    

    spark_operation_task = BashOperator(
        task_id ='run_spark_etl_job',
        bash_command='python /opt/airflow/scripts/spark_etl.py',

    )

    sensing_transformed_fact_data_task = FileSensor(
        task_id='sensing_transformed_fact_data',
        fs_conn_id='prescriber_default',
        filepath='/opt/airflow/staging/top5_prescribers.csv',
        poke_interval=10,
        
    )

    sensing_transformed_city_data_task = FileSensor(
        task_id='sensing_transformed_city_data',
        fs_conn_id='city_default',
        filepath='/opt/airflow/staging/city.csv',
        poke_interval=10,
        
    )

    loading_prescriber_report_toS3_task = PythonOperator(
        task_id = 'loading_prescriber_report_to_S3',
        python_callable=local_filesTo_s3,
        op_kwargs={
            'filepath': '/opt/airflow/staging/top5_prescribers.csv',
            'bucket_name': creds['s3_bucketname_prsc'],
           # 'key': str(Variable.get('key'))
            'key': 'prescribers'
            },
    )


    loading_city_report_toS3_task = PythonOperator(
        task_id = 'loading_city_report_to_S3',
        python_callable=local_filesTo_s3,
        op_kwargs={
            'filepath': '/opt/airflow/staging/city.csv',
            'bucket_name': creds['s3_bucketname_city'],
           # 'key': str(Variable.get('key'))
            'key': 'city_report'
            },
    )

    loading_prescriber_to_Postgres_task = PythonOperator(
        task_id = 'loading_prescriber_report_to_postgres',
        python_callable=load_files_to_DB,
        op_kwargs={
            'file_name': 'top5_prescribers.csv',
            'data_path': '/opt/airflow/staging/top5_prescribers.csv',

            }, 
    )

    loading_city_report_to_Postgres_task = PythonOperator(
        task_id = 'loading_city_report_to_postgres',
        python_callable=load_files_to_DB,
        op_kwargs={
            'file_name': 'city.csv',
            'data_path': '/opt/airflow/staging/city.csv',

            },  
    )

    loading_city_report_to_azure_task = PythonOperator(
        task_id = 'loading_city_report_to_azure_blob',
        python_callable=local_to_blob_storage,
        op_kwargs={
            'file_path': '/opt/airflow/staging/top5_prescribers.csv',
            'prefix': 'top5_presc',
            'file_name': 'top5_prescribers.csv'

            }, 
    )

    loading_prescribers_report_to_azure_task = PythonOperator(
        task_id = 'loading_prescriber_report_to_azure_blob',
        python_callable=local_to_blob_storage,
        op_kwargs={
            'file_path': '/opt/airflow/staging/city.csv',
            'prefix': 'city',
            'file_name': 'city.csv'

            }, 
    )

    data_quality_check_task = PythonOperator(
        task_id = 'data_quality_checks',
        python_callable=data_quality_checks,
        op_kwargs={'tables': 'prescriber_report, city_report'},
        
    )

    start_execution_task = DummyOperator(
        task_id = 'start_execution',
        
    )

    transformed_data_ready_task = DummyOperator(
        task_id = 'transformed_data_ready',
        
    )

    loaded_data_ready_task = DummyOperator(
        task_id = 'loaded_data_ready',
        
    )

    clean_up_task = PythonOperator(
        task_id = 'clean_up_process',
        python_callable=cleaning_process,
    )

    end_execution_task = DummyOperator(
        task_id = 'end_execution',
        
    )





    start_execution_task >> spark_operation_task >> [sensing_transformed_city_data_task, 
                                                    sensing_transformed_fact_data_task]

    [sensing_transformed_city_data_task, 
    sensing_transformed_fact_data_task] >> transformed_data_ready_task
    transformed_data_ready_task >> [loading_prescriber_report_toS3_task,
                                    loading_city_report_toS3_task,
                                    loading_city_report_to_Postgres_task,
                                    loading_prescriber_to_Postgres_task,
                                    loading_city_report_to_azure_task,
                                    loading_prescribers_report_to_azure_task]
    [loading_prescriber_report_toS3_task,
     loading_city_report_toS3_task,
     loading_city_report_to_Postgres_task,
     loading_prescriber_to_Postgres_task,
     loading_city_report_to_azure_task,
     loading_prescribers_report_to_azure_task] >> loaded_data_ready_task >> data_quality_check_task

    data_quality_check_task >> clean_up_task >> end_execution_task
