import sys
import os
import pandas as pd
from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Assuming helper_functions.py is accessible via Airflow's PythonPath
# or located in the same DAGs folder.
from helper_functions import read_s3_csv_to_df, write_df_to_s3_csv 

# --- S3-based Log for Processed Streams Files (Recommended for MWAA) ---
# This approach stores the list of processed S3 keys in a text file within an S3 bucket.
# This S3 bucket and key should be configured in etl_dag.py and passed to the callable.

def get_processed_files_s3(log_bucket: str, log_key: str, aws_conn_id: str = 'aws_default') -> set:
    """Reads the list of already processed S3 keys from an S3 log file."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        if s3_hook.check_for_key(key=log_key, bucket_name=log_bucket):
            log_content = s3_hook.read_key(key=log_key, bucket_name=log_bucket)
            return set(line.strip() for line in log_content.splitlines() if line.strip())
        else:
            print(f"S3 log file s3://{log_bucket}/{log_key} not found. Starting with empty processed files list.")
            return set()
    except Exception as e:
        print(f"[WARNING] Could not read S3 processed files log from s3://{log_bucket}/{log_key}: {e}")
        return set()

def add_processed_file_s3(log_bucket: str, log_key: str, s3_key_to_add: str, aws_conn_id: str = 'aws_default'):
    """Adds an S3 key to the S3 log of processed files."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        # Read current log, add new key, and write back
        processed_files = get_processed_files_s3(log_bucket, log_key, aws_conn_id)
        processed_files.add(s3_key_to_add)
        updated_log_content = "\n".join(sorted(list(processed_files))) # Sort for consistency
        s3_hook.load_string(string_data=updated_log_content, key=log_key, bucket_name=log_bucket, replace=True)
        print(f"Updated S3 processed files log with: {s3_key_to_add}")
    except Exception as e:
        print(f"[ERROR] Failed to update S3 processed files log s3://{log_bucket}/{log_key}: {e}")
        raise # Re-raise to fail the task if logging fails

def find_new_streams_file_callable(bucket_name: str, prefix: str, log_bucket: str, log_key: str, ti, **kwargs) -> dict or None:
    """
    Checks for new, unprocessed streams files in the specified S3 bucket and prefix.
    Uses an S3-based log to track processed files.
    """
    print(f"Checking for new files in s3://{bucket_name}/{prefix}")
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    processed_files = get_processed_files_s3(log_bucket, log_key)
    print(f"Found {len(processed_files)} previously processed files in S3 log.")

    all_current_files = set()
    try:
        # List files in the S3 source prefix
        # Use delimiter='/' and then filter for files (not 'folders')
        # This approach lists all objects under the prefix, including sub-directories if any.
        # Ensure your source files are directly under 'streams/' or adjust prefix/key logic.
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix, delimiter='/')
        if keys:
            # Filter out keys that end with '/', which typically represent folders
            all_current_files = set(k for k in keys if not k.endswith('/'))
        else:
            print(f"No files found under s3://{bucket_name}/{prefix}.")
            ti.xcom_push(key='streams_pipeline_status', value='no_new_files')
            return None # No files at all
            
    except Exception as e:
        raise AirflowException(f"Failed to list keys in s3://{bucket_name}/{prefix}: {e}")

    new_files_to_process = sorted(list(all_current_files - processed_files))

    if new_files_to_process:
        # For simplicity, we process only the first new file found in this example.
        # In a real-world scenario, you might iterate and process all new files
        # or trigger multiple parallel tasks.
        new_s3_key = new_files_to_process[0]
        print(f"Found new unprocessed streams file: {new_s3_key}")
        
        # Add the new file to the processed log before proceeding
        add_processed_file_s3(log_bucket, log_key, new_s3_key)

        s3_location_info = {
            'bucket_name': bucket_name,
            's3_key': new_s3_key
        }
        ti.xcom_push(key='streams_pipeline_status', value='new_file_found')
        return s3_location_info
    else:
        print(f"No new unprocessed streams files found in s3://{bucket_name}/{prefix}. Skipping streams pipeline.")
        ti.xcom_push(key='streams_pipeline_status', value='no_new_files')
        return None

def decide_streams_pipeline_path(ti, **kwargs):
    """
    Branches the DAG based on whether new streams files were found.
    """
    streams_pipeline_status = ti.xcom_pull(task_ids='find_new_streams_file', key='streams_pipeline_status')
    if streams_pipeline_status == 'new_file_found':
        print("New streams file found. Proceeding with streams pipeline.")
        return 'ingest_and_stage_streams_data_task' # Name of the next task if new file found
    else:
        print("No new streams files found. Skipping streams pipeline.")
        return 'dummy_skip_streams_pipeline' # Name of the task to skip to
    
def ingest_and_stage_streams_data_from_s3(output_s3_prefix: str, ti, S3_STAGING_BUCKET_NAME: str):
    """
    Reads streams data from the source S3 bucket and writes it to a raw stage
    in the staging S3 bucket.
    """
    # Pull source_s3_location_info directly from XCom inside the callable
    # This ensures it's received as a Python dictionary.
    source_s3_location_info = ti.xcom_pull(task_ids='find_new_streams_file', key='return_value')

    if not (source_s3_location_info and isinstance(source_s3_location_info, dict) and 'bucket_name' in source_s3_location_info and 's3_key' in source_s3_location_info):
        raise AirflowException("No valid source S3 location info (or not a dictionary) for streams ingestion.")

    source_bucket_name = source_s3_location_info['bucket_name']
    source_s3_key = source_s3_location_info['s3_key']

    full_s3_path = f"s3://{source_bucket_name}/{source_s3_key}"
    print(f"Attempting to ingest and stage raw streams data from {full_s3_path}")

    try:
        df = read_s3_csv_to_df(source_bucket_name, source_s3_key)
        print(f"Successfully read {len(df)} rows from {full_s3_path}.")

        file_name = source_s3_key.split('/')[-1]
        output_s3_key = f"{output_s3_prefix}{file_name}"

        write_df_to_s3_csv(df, S3_STAGING_BUCKET_NAME, output_s3_key)

        staged_s3_location_info = {
            'bucket_name': S3_STAGING_BUCKET_NAME,
            's3_key': output_s3_key
        }

        print(f"Successfully staged raw streams data to s3://{S3_STAGING_BUCKET_NAME}/{output_s3_key}.")
        return staged_s3_location_info

    except Exception as e:
        print(f"Error during streams ingestion and staging from {full_s3_path}: {e}")
        raise

def ingest_and_stage_static_data_from_s3(bucket_name, s3_key, output_s3_prefix, ti, S3_STAGING_BUCKET_NAME):
    """
    Ingests static data (like user or song data) from a specified S3 bucket and key,
    then writes it to a raw stage in the staging S3 bucket.
    """
    full_s3_path = f"s3://{bucket_name}/{s3_key}"
    print(f"Attempting to ingest and stage static data from {full_s3_path}")

    try:
        df = read_s3_csv_to_df(bucket_name, s3_key)
        print(f"Successfully read {len(df)} rows from {full_s3_path}.")
        
        file_name = s3_key.split('/')[-1]
        output_s3_key = f"{output_s3_prefix}{file_name}"

        write_df_to_s3_csv(df, S3_STAGING_BUCKET_NAME, output_s3_key)
        
        staged_s3_location_info = {
            'bucket_name': S3_STAGING_BUCKET_NAME,
            's3_key': output_s3_key
        }
        ti.xcom_push(key=f'staged_s3_location_info_{file_name.replace(".", "_").replace("-", "_")}', value=staged_s3_location_info)
        
        print(f"Successfully staged raw static data to s3://{S3_STAGING_BUCKET_NAME}/{output_s3_key}.")
        return staged_s3_location_info
    except Exception as e:
        print(f"Error during static data ingestion and staging from {full_s3_path}: {e}")
        raise