# import pandas as pd
from airflow.exceptions import AirflowException
from helper_functions import read_s3_csv_to_df, write_df_to_s3_csv # Import from helper_functions.py

def validate_columns_logic(df, required_columns, file_name):
    """
    Core validation logic to check for missing columns in a DataFrame.
    Returns True if all required columns are present, False otherwise.
    """
    actual_columns = set(df.columns)
    missing_columns = required_columns - actual_columns
    if missing_columns:
        print(f"[ERROR] {file_name} is missing columns: {missing_columns}")
        return False
    else:
        print(f"[OK] {file_name} has all required columns.")
        return True

def check_null_values(df, columns_to_check, file_name):
    """
    Checks for null values in specified columns of a DataFrame.
    Prints warnings for columns with nulls but does not raise an exception.
    Returns True if NO nulls are found in specified columns, False if ANY nulls are found.
    """
    null_found_in_any_critical_col = False
    for col in columns_to_check:
        if col in df.columns:
            if df[col].isnull().any():
                null_count = df[col].isnull().sum()
                print(f"[WARNING] {file_name}: Column '{col}' contains {null_count} null values.")
                null_found_in_any_critical_col = True
        else:
            print(f"[WARNING] {file_name}: Column '{col}' not found for null check. (Should be caught by column validation).")
    
    if not null_found_in_any_critical_col:
        print(f"[OK] {file_name} has no null values in the explicitly checked columns.")
    return not null_found_in_any_critical_col

def validate_data_task_callable(ti, input_task_id, required_cols, critical_null_cols, file_name, output_s3_prefix, S3_STAGING_BUCKET_NAME):
    """
    Generalized callable for validation tasks.
    It pulls data's S3 location from XCom, reads the data, validates columns,
    checks for nulls (logs but doesn't fail), and writes validated data back to S3.
    """
    s3_location_info = ti.xcom_pull(task_ids=input_task_id, key='return_value')
    
    if not (s3_location_info and 'bucket_name' in s3_location_info and 's3_key' in s3_location_info):
        raise AirflowException(f"No valid S3 location info from task '{input_task_id}' found in XCom.")
    
    input_bucket_name = s3_location_info['bucket_name']
    input_s3_key = s3_location_info['s3_key']
    
    print(f"Attempting to read {file_name} for validation from s3://{input_bucket_name}/{input_s3_key}")
    try:
        df = read_s3_csv_to_df(input_bucket_name, input_s3_key)
        print(f"Successfully read {len(df)} rows for {file_name} validation.")
    except Exception as e:
        raise AirflowException(f"Failed to read {file_name} for validation from S3: {e}")

    # Perform column validation (will fail the task if missing columns)
    if not validate_columns_logic(df, required_cols, file_name):
        raise AirflowException(f"{file_name} column validation failed: missing columns. Task will abort.")
    
    # Perform null value check (logs warnings, but does not fail the task)
    if not check_null_values(df, critical_null_cols, file_name):
        print(f"[INFO] {file_name} has null values in critical columns ({critical_null_cols}). These rows will be handled (e.g., dropped) in the subsequent transformation step.")
    else:
        print(f"[OK] {file_name} has no nulls in critical columns after validation check.")
    
    print(f"{file_name} validation complete. Writing validated data to S3.")

    # Write validated data to the appropriate S3 staging prefix
    output_s3_key = f"{output_s3_prefix}{input_s3_key.split('/')[-1]}"
    write_df_to_s3_csv(df, S3_STAGING_BUCKET_NAME, output_s3_key)
    
    validated_s3_location_info = {
        'bucket_name': S3_STAGING_BUCKET_NAME,
        's3_key': output_s3_key
    }
    
    print(f"Pushed validated {file_name} S3 path to XCom: s3://{S3_STAGING_BUCKET_NAME}/{output_s3_key}")
    return validated_s3_location_info
