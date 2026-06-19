import sys
import pathlib
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
import logging
import time
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import numpy as np
import pandas as pd
from pandas import json_normalize
import pytz
import glob
import boto3
import airflow
from sqlalchemy import create_engine
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from library.general_func import *

local_config = Variable.get("dag_get_gatec_db", deserialize_json=True)
default_args_config = local_config["default_args"]
aws_config = Variable.get("aws_service_account", deserialize_json=True)

database_name = local_config['database_name']
output_bucket = local_config['output_bucket']
temp_folder = local_config['temp_folder']
bucket_name = local_config['bucket_name']
extract_date = local_config['extract_date']
sql_conn = local_config['sql_conn']

ACCESS_KEY = aws_config["ACCESS_KEY"]
SECRET_KEY = aws_config["SECRET_KEY"]

def extract_by_table(schema_name, table_name, engine):

    query = f"select * from {schema_name}.{table_name}"

    df = pd.read_sql(query, engine)
    df['json_field'] = df.to_json(orient='records', index=False, lines=True, date_format='iso').splitlines()
    df = df[['json_field']]

    df.to_parquet(f'{temp_folder}/{schema_name}_{table_name}_{extract_date}.parquet')

    upload_file_to_s3(local_file_path=f"{temp_folder}/{schema_name}_{table_name}_{extract_date}.parquet", 
                      bucket_name=bucket_name, 
                      object_key=f"GATecDB/{schema_name}/{table_name}/extract_date={extract_date}/{schema_name}_{table_name}_{extract_date}.parquet", 
                      ACCESS_KEY=ACCESS_KEY, 
                      SECRET_KEY=SECRET_KEY)
    
    if check_table_exists(database_name=database_name, 
                          table_name=f"{schema_name}_{table_name}", 
                          ACCESS_KEY=ACCESS_KEY, 
                          SECRET_KEY=SECRET_KEY, 
                          catalog='AwsDataCatalog'):
        print('table ada')
    else:
        print('table belum ada')
        create_table_if_not_exist(table_name=f"{schema_name}_{table_name}", 
                                  s3_path=f"{bucket_name}/GATecDB/{schema_name}/{table_name}/", 
                                  database_name=database_name, 
                                  output_bucket=output_bucket, 
                                  ACCESS_KEY=ACCESS_KEY, 
                                  SECRET_KEY=SECRET_KEY)
    

    os.remove(f'{temp_folder}/{schema_name}_{table_name}_{extract_date}.parquet')

    query_alter = f"alter table {database_name}.{schema_name}_{table_name} add partition (extract_date = '{extract_date}')"

    create_table_partition_if_not_exist_on_athena(query_alter=query_alter, 
                                                  database_name=database_name, 
                                                  output_bucket=output_bucket, 
                                                  ACCESS_KEY=ACCESS_KEY, 
                                                  SECRET_KEY=SECRET_KEY)
    

def extract_by_query(file_location, report_type, report_name, engine):

    with open(f"{file_location}", "r") as qf:
        query = qf.read()
    
    df = pd.read_sql(query, engine)
    df['json_field'] = df.to_json(orient='records', index=False, lines=True, date_format='iso').splitlines()
    df = df[['json_field']]
    
    df.to_parquet(f"{temp_folder}/{report_type}_{report_name}_{extract_date}.parquet")

    upload_file_to_s3(local_file_path=f"{temp_folder}/{report_type}_{report_name}_{extract_date}.parquet", 
                      bucket_name=bucket_name, 
                      object_key=f"GATecDB/{report_type}/{report_name}/extract_date={extract_date}/{report_type}_{report_name}_{extract_date}.parquet", 
                      ACCESS_KEY=ACCESS_KEY, 
                      SECRET_KEY=SECRET_KEY)
    
    if check_table_exists(database_name=database_name, 
                          table_name=f"{report_type}_{report_name}", 
                          ACCESS_KEY=ACCESS_KEY, 
                          SECRET_KEY=SECRET_KEY, 
                          catalog='AwsDataCatalog'):
        print('table ada')
    else:
        print('table belum ada')
        create_table_if_not_exist(table_name=f"{report_type}_{report_name}", 
                                  s3_path=f"{bucket_name}/GATecDB/{report_type}/{report_name}/", 
                                  database_name=database_name, 
                                  output_bucket=output_bucket, 
                                  ACCESS_KEY=ACCESS_KEY, 
                                  SECRET_KEY=SECRET_KEY)
    

    os.remove(f"{temp_folder}/{report_type}_{report_name}_{extract_date}.parquet")

    query_alter = f"""alter table {database_name}.{report_type}_{report_name} add
                      partition (extract_date = '{extract_date}');"""

    create_table_partition_if_not_exist_on_athena(query_alter=query_alter, 
                                                  database_name=database_name, 
                                                  output_bucket=output_bucket, 
                                                  ACCESS_KEY=ACCESS_KEY, 
                                                  SECRET_KEY=SECRET_KEY)


def set_extract_date_for_time_partition(local_config):

    local_config['extract_date'] = str((datetime.now() + timedelta(hours=7)).date())
    
    local_config = json.dumps(local_config, indent=4)

    Variable.set("dag_get_gatec_db", local_config)

default_args = {
   'owner': default_args_config["owner"],
   'depends_on_past': default_args_config["depends_on_past"],
   'start_date': datetime.strptime(default_args_config["start_date"], default_args_config["start_date_format"]),
   'email': default_args_config["email"],
   'email_on_failure': default_args_config["email_on_failure"],
   'email_on_retry': default_args_config["email_on_retry"],
   'retries': default_args_config["retries"],
   'retry_delay': timedelta(minutes=default_args_config["retry_delay"])
}

with DAG(
    dag_id=local_config["dag_name"],
    default_args=default_args,
    catchup=default_args_config["catchup"],
    schedule=default_args_config["schedule_interval"]
) as dag:
    
    task_start = EmptyOperator(
        task_id="start",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    task_end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    task_set_extract_date_for_time_partition = PythonOperator(
        task_id=f"task_set_extract_date_for_time_partition",
        python_callable=set_extract_date_for_time_partition,
        op_kwargs={
            'local_config': local_config
        }
    )

    engine = create_engine(sql_conn)
    
    with TaskGroup(group_id=f'process_extract_by_table') as tg:

        task_start_extract_by_table = EmptyOperator(
            task_id="start_extract_by_table",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        for x in local_config['table_list']:

            task_extract_by_table = PythonOperator(
                task_id=f"task_extract_by_table_{x['schema_name']}_{x['table_name']}",
                python_callable=extract_by_table,
                op_kwargs={
                    'schema_name': x['schema_name'], 
                    'table_name': x['table_name'], 
                    'engine': engine
                }
            )

            task_start >> task_start_extract_by_table >> task_extract_by_table >> task_end
    
    with TaskGroup(group_id=f'process_extract_by_query') as tg:

        task_start_extract_by_query = EmptyOperator(
            task_id="start_extract_by_query",
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

        for f in local_config['query_file_list']:

            task_extract_by_query = PythonOperator(
                task_id=f"task_extract_by_table_{f['report_type']}_{f['report_name']}",
                python_callable=extract_by_query,
                op_kwargs={
                    'file_location': f['file_location'], 
                    'report_type': f['report_type'], 
                    'report_name': f['report_name'], 
                    'engine': engine
                }
            )

            task_start >> task_start_extract_by_query >> task_extract_by_query >> task_end


    task_end >> task_set_extract_date_for_time_partition
