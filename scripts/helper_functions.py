from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
import pandas as pd
import io

def read_s3_csv_to_df(bucket_name, s3_key, aws_conn_id='aws_default'):
    """
    Helper function to read a CSV file from S3 into a pandas DataFrame.
    Uses Airflow's S3Hook for AWS S3 connectivity.
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        csv_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
        df = pd.read_csv(io.StringIO(csv_content))
        print(f"Read {len(df)} rows from s3://{bucket_name}/{s3_key}")
        return df
    except Exception as e:
        raise AirflowException(f"Failed to read CSV from s3://{bucket_name}/{s3_key}: {e}")

def write_df_to_s3_csv(df, bucket_name, s3_key, aws_conn_id='aws_default'):
    """
    Helper function to write a pandas DataFrame to S3 as a CSV file.
    Uses Airflow's S3Hook for AWS S3 connectivity.
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True # Overwrite if exists
        )
        print(f"Successfully wrote data to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        raise AirflowException(f"Failed to write CSV to s3://{bucket_name}/{s3_key}: {e}")

