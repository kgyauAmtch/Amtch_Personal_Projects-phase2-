# load.py

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # Used for Redshift (PostgreSQL compatible)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # Used for S3 cleanup
from airflow.models.dag import DAG
from airflow.exceptions import AirflowException
from typing import Optional, List


def _execute_redshift_sql_callable(sql_commands: str | list[str], redshift_conn_id: str):
    """
    Callable to execute SQL commands in Redshift using PostgresHook.
    Handles single SQL strings or a list of SQL strings.
    """
    hook = PostgresHook(postgres_conn_id=redshift_conn_id)
    if isinstance(sql_commands, str):
        sql_commands = [sql_commands] # Ensure it's a list for iteration

    for sql_command in sql_commands:
        if sql_command.strip(): # Only execute non-empty commands
            print(f"Executing SQL in Redshift: {sql_command}")
            hook.run(sql_command)
        else:
            print("Skipping empty SQL command.")
    print("Redshift SQL execution complete.")


def _copy_s3_to_redshift_callable(
    ti, # Added ti to receive TaskInstance context
    s3_key_upstream_task_id: str, # New: Task ID of the upstream task producing the S3 key
    s3_key_xcom_key: str,       # New: XCom key to pull the S3 path dictionary
    s3_fallback_bucket: str,    # New: Original bucket for fallback if XCom doesn't provide it
    redshift_table: str,
    redshift_iam_role_arn: str,
    redshift_conn_id: str,
    copy_options: Optional[List[str]] = None,
    target_columns: Optional[List[str]] = None
):
    """
    Callable to perform a COPY command from S3 to Redshift using PostgresHook.
    Pulls the actual S3 key and bucket from XCom.
    """
    if copy_options is None:
        copy_options = ['csv', 'IGNOREHEADER 1']

    copy_options_str = " ".join(copy_options).upper()

    # Pull the S3 path dictionary from XCom
    s3_info_dict = ti.xcom_pull(task_ids=s3_key_upstream_task_id, key=s3_key_xcom_key)

    if not (isinstance(s3_info_dict, dict) and 's3_key' in s3_info_dict and 'bucket_name' in s3_info_dict):
        raise AirflowException(f"Could not retrieve valid S3 key info from XCom for task {s3_key_upstream_task_id} with key {s3_key_xcom_key}. Received: {s3_info_dict}")

    actual_s3_key = s3_info_dict['s3_key']
    actual_s3_bucket = s3_info_dict['bucket_name'] # Use bucket from XCom for accuracy

    # Construct the column list for the COPY command if provided
    columns_clause = ""
    if target_columns:
        columns_clause = f"({', '.join(target_columns)})"

    # Construct the COPY SQL command
    copy_sql = f"""
        COPY {redshift_table} {columns_clause}
        FROM 's3://{actual_s3_bucket}/{actual_s3_key}'
        IAM_ROLE '{redshift_iam_role_arn}'
        {copy_options_str};
    """
    print(f"Executing Redshift COPY command:\n{copy_sql}")
    _execute_redshift_sql_callable(copy_sql, redshift_conn_id)
    print(f"COPY command for {redshift_table} from s3://{actual_s3_bucket}/{actual_s3_key} completed.")


def _delete_s3_key_callable(ti, s3_key_upstream_task_id: str, s3_key_xcom_key: str, aws_conn_id: str):
    """
    Callable to delete a specific S3 object using S3Hook.
    It expects the s3_key to be an XCom template that needs to be rendered.
    """
    s3_info_dict = ti.xcom_pull(task_ids=s3_key_upstream_task_id, key=s3_key_xcom_key)

    if not (isinstance(s3_info_dict, dict) and 's3_key' in s3_info_dict and 'bucket_name' in s3_info_dict):
        print(f"[WARNING] S3 cleanup skipped: Could not retrieve valid S3 key info from XCom for task {ti.task_id}. "
              f"Received: {s3_info_dict}. Manual cleanup may be required.")
        return # Do not attempt deletion if XCom data is missing or malformed

    actual_s3_key = s3_info_dict['s3_key']
    actual_bucket_name = s3_info_dict['bucket_name']

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    print(f"Attempting to delete s3://{actual_bucket_name}/{actual_s3_key}")
    try:
        s3_hook.delete_objects(bucket=actual_bucket_name, keys=actual_s3_key)
        print(f"Successfully deleted s3://{actual_bucket_name}/{actual_s3_key}")
    except Exception as e:
        print(f"[ERROR] Failed to delete s3://{actual_bucket_name}/{actual_s3_key}: {e}")


def create_redshift_load_and_upsert_tasks(
    dag: DAG,
    task_prefix: str,
    s3_bucket: str, # This is used as a fallback bucket or for general reference
    s3_key_upstream_task_id: str, # New: Task ID of the upstream task producing the S3 key
    s3_key_xcom_key: str,       # New: XCom key to pull the S3 path dictionary
    redshift_staging_table: str,
    redshift_final_table: str,
    redshift_primary_keys: list[str],
    redshift_conn_id: str = 'redshift_default',
    redshift_iam_role_arn: str = None,
    redshift_table_columns: Optional[List[str]] = None
):
    """
    Creates and returns Airflow tasks for Redshift data loading (COPY) and upserting (DELETE/INSERT),
    and an S3 cleanup task. This helper now assumes table creation/truncation are handled externally.
    """
    if redshift_iam_role_arn is None:
        raise AirflowException("redshift_iam_role_arn must be provided for Redshift COPY command.")

    load_task_id = f'load_{task_prefix}_to_redshift_staging'
    upsert_task_id = f'upsert_{task_prefix}_to_redshift_final'
    cleanup_task_id = f'cleanup_s3_{task_prefix}_files'

    # The load task: copies data from S3 to Redshift staging table using PythonOperator
    load_task = PythonOperator(
        task_id=load_task_id,
        python_callable=_copy_s3_to_redshift_callable,
        op_kwargs={
            's3_key_upstream_task_id': s3_key_upstream_task_id,
            's3_key_xcom_key': s3_key_xcom_key,
            's3_fallback_bucket': s3_bucket,
            'redshift_table': redshift_staging_table,
            'redshift_iam_role_arn': redshift_iam_role_arn,
            'redshift_conn_id': redshift_conn_id,
            'copy_options': ['csv', 'IGNOREHEADER 1'],
            'target_columns': redshift_table_columns,
        },
        provide_context=True, # Essential for _copy_s3_to_redshift_callable to access ti
        dag=dag,
    )

    # Construct the WHERE clause for the DELETE statement based on primary keys
    pk_join_condition = " AND ".join([
        f"{redshift_final_table}.{pk} = {redshift_staging_table}.{pk}" for pk in redshift_primary_keys
    ])

    # Redshift-compliant UPSERT SQL
    upsert_sql = f"""
        BEGIN;

        -- Delete existing records in the final table that match primary keys in the staging table
        DELETE FROM {redshift_final_table}
        USING {redshift_staging_table}
        WHERE {pk_join_condition};

        -- Insert all records from the staging table into the final table
        INSERT INTO {redshift_final_table}
        SELECT * FROM {redshift_staging_table};

        -- Truncate the staging table is now handled by an explicit PostgresOperator
        -- TRUNCATE TABLE {redshift_staging_table};

        COMMIT;
    """

    # Task to execute the upsert SQL using PythonOperator
    upsert_task = PythonOperator(
        task_id=upsert_task_id,
        python_callable=_execute_redshift_sql_callable,
        op_kwargs={
            'sql_commands': upsert_sql,
            'redshift_conn_id': redshift_conn_id,
        },
        dag=dag,
    )

    # Task to delete the temporary S3 files after Redshift loading and upsert
    s3_cleanup_task = PythonOperator(
        task_id=cleanup_task_id,
        python_callable=_delete_s3_key_callable,
        op_kwargs={
            's3_key_upstream_task_id': s3_key_upstream_task_id,
            's3_key_xcom_key': s3_key_xcom_key,
            'aws_conn_id': 'aws_default',
        },
        provide_context=True, # Essential for _delete_s3_key_callable to access ti
        trigger_rule='all_done', # Run even if upsert_task is skipped or fails, to clean up S3
        dag=dag,
    )

    # Set dependencies: load_task must complete before upsert_task, and cleanup runs after upsert
    # Table creation/truncation dependencies are now expected to be handled externally in the DAG.
    load_task >> upsert_task >> s3_cleanup_task

    return load_task, upsert_task, s3_cleanup_task
