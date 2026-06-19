import boto3
import requests
import pandas as pd
import io
import json
from pandas import json_normalize
import time
import xml.etree.ElementTree as ET

def upload_file_to_s3(local_file_path, bucket_name, object_key, ACCESS_KEY, SECRET_KEY):
    try:
        # Create a file-like object from the string data
        # Use io.BytesIO if your data is binary or needs explicit encoding
        # file_object = io.StringIO(data_to_upload)
    
        # Initialize the S3 client
        s3_client = boto3.client('s3', 
                                  aws_access_key_id=ACCESS_KEY,
                                  aws_secret_access_key=SECRET_KEY)
    
        s3_client.upload_file(local_file_path, bucket_name, object_key)
        print(f"File '{local_file_path}' uploaded successfully to '{object_key}' in bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error uploading file: {e}")

def check_table_exists(database_name, table_name, ACCESS_KEY, SECRET_KEY, catalog='AwsDataCatalog'):
    glue = boto3.client('glue',
                        region_name='ap-southeast-3',
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False

def get_data_api(url, write_path, headers, payload, method):

    response = requests.request(method, url, headers=headers, data=payload)

    json_data = {
        "json_field": response.text
    }

    df = json_normalize(json_data)

    df.to_parquet(write_path, index=False)

    return response

def check_athena_partition_exists(database_name, table_name, partition_values, ACCESS_KEY, SECRET_KEY):
    """
    Checks if an AWS Athena partition exists in the Glue Data Catalog.

    Args:
        database_name (str): The name of the Athena database.
        table_name (str): The name of the Athena table.
        partition_values (list): A list of strings representing the partition values
                                 in the order of the partition keys.

    Returns:
        bool: True if the partition exists, False otherwise.
    """
    glue_client = boto3.client('glue',
                               region_name='ap-southeast-3',
                               aws_access_key_id=ACCESS_KEY,
                               aws_secret_access_key=SECRET_KEY)

    try:
        response = glue_client.get_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=partition_values
        )
        print(f"Partition {partition_values} exists in table {table_name}.")
        return True
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Partition {partition_values} does not exist in table {table_name}.")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def get_aws_athena_data_as_json(database, output_bucket, query, ACCESS_KEY, SECRET_KEY):
    athena = boto3.client('athena', 
                          region_name='ap-southeast-3',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)  # e.g., 'ap-southeast-1'
    
    # Start query
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': f's3://{output_bucket}'}
    )
    query_execution_id = response['QueryExecutionId']
    
    # Wait for query to complete
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    # Get results as DataFrame
    results_paginator = athena.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(QueryExecutionId=query_execution_id)
    
    rows = []
    columns = []
    
    for i, results_page in enumerate(results_iter):
        for row_idx, row in enumerate(results_page['ResultSet']['Rows']):
            data = [col.get('VarCharValue', None) for col in row['Data']]
            if i == 0 and row_idx == 0:
                columns = data
            elif row_idx != 0 or i != 0:
                rows.append(dict(zip(columns, data)))
    
    # Convert to JSON
    json_data = json.dumps(rows, indent=2)
    
    # Print or use json_data
    return json_data

def create_table_partition_if_not_exist_on_athena(query_alter, database_name, output_bucket, ACCESS_KEY, SECRET_KEY):

    client = boto3.client('athena', 
                      region_name='ap-southeast-3',
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    queryStart = client.start_query_execution(
        QueryString = query_alter,
        QueryExecutionContext = {
            'Database': f'{database_name}'
        },
        ResultConfiguration = { 'OutputLocation': f's3://{output_bucket}'}
    )

def create_table_if_not_exist(table_name, s3_path, database_name, output_bucket, ACCESS_KEY, SECRET_KEY):

    client = boto3.client('athena', 
                      region_name='ap-southeast-3',
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    query = f"""
        CREATE EXTERNAL TABLE {table_name}(
          `json_field` string)
        PARTITIONED BY ( 
          `extract_date` date)
        ROW FORMAT SERDE 
          'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
        STORED AS INPUTFORMAT 
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
        OUTPUTFORMAT 
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION
          's3://{s3_path}'
        TBLPROPERTIES (
          'classification'='parquet', 
          'parquet.compression'='SNAPPY'
        )
    """

    queryStart = client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database': f'{database_name}'
        },
        ResultConfiguration = { 'OutputLocation': f's3://{output_bucket}'}
    )    

    msck_query = f"""msck repair table {table_name}"""

    queryStart = client.start_query_execution(
        QueryString = msck_query,
        QueryExecutionContext = {
            'Database': f'{database_name}'
        },
        ResultConfiguration = { 'OutputLocation': f's3://{output_bucket}'}
    )

def xml_to_json_clean(xml_string: str):
    def strip_namespace(tag):
        """Remove namespace like {http://...}tag → tag"""
        if tag.startswith("{"):
            return tag.split("}", 1)[1]
        return tag

    def elem_to_dict(elem):
        """Convert XML element to dict, removing namespaces"""
        node = {}
        # Add element attributes (if any)
        for k, v in elem.attrib.items():
            node[strip_namespace(k)] = v

        # Add child elements
        for child in elem:
            child_dict = elem_to_dict(child)
            tag = strip_namespace(child.tag)
            if tag in node:
                # Handle multiple children with same tag
                if not isinstance(node[tag], list):
                    node[tag] = [node[tag]]
                node[tag].append(child_dict)
            else:
                node[tag] = child_dict

        # Add text if it exists and is not just whitespace
        text = (elem.text or "").strip()
        if text and not node:
            return text
        elif text:
            node["_text"] = text

        return node

    # Parse XML safely
    root = ET.fromstring(xml_string)
    data = {strip_namespace(root.tag): elem_to_dict(root)}
    return data

def get_data_api_xml(url, write_path, headers, payload, method):

    response = requests.request(method, url, headers=headers, data=payload)

    data = xml_to_json_clean(response.text)

    json_data = {
        "json_field": json.dumps(data, indent=2)
    }

    df = json_normalize(json_data)

    df.to_parquet(write_path, index=False)

    return data