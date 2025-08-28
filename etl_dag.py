from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Import callables from modularized files
from extraction import (
    find_new_streams_file_callable,
    decide_streams_pipeline_path,
    ingest_and_stage_streams_data_from_s3,
    ingest_and_stage_static_data_from_s3
)
from validation import validate_data_task_callable
from transformation import transform_data_task_callable, calculate_genre_kpis_callable
from load import create_redshift_load_and_upsert_tasks


# --- S3 Configuration ---
S3_STREAMS_BUCKET_NAME = 'lab1-etl-landingzone'
S3_STREAMS_RAW_PREFIX = 'streams/'

S3_USER_SONG_BUCKET_NAME = 'etl-lab1-rds-landingzone'
S3_KEY_USER_RAW = 'processed/users_data.csv'
S3_KEY_SONG_RAW = 'processed/songs_data.csv'

S3_STAGING_BUCKET_NAME = 'staging-bucket-lab1test'

S3_PREFIX_STREAMS_RAW_STAGED = 'raw/streams/'
S3_PREFIX_STREAMS_VALIDATED = 'validated/streams/'
S3_PREFIX_STREAMS_TRANSFORMED = 'transformed/streams/'

S3_PREFIX_HOURLY_KPI = 'transformed/hourly_kpis/'
S3_PREFIX_GENRE_KPI = 'transformed/genre_kpis/'

S3_PREFIX_USER_VALIDATED = 'validated/users/'
S3_PREFIX_USER_TRANSFORMED = 'transformed/users/'

S3_PREFIX_SONG_VALIDATED = 'validated/songs/'
S3_PREFIX_SONG_TRANSFORMED = 'transformed/songs/'

# --- S3 Configuration for Processed Streams Log ---
S3_PROCESSED_LOG_BUCKET = 'lab1-etl-landingzone'
S3_PROCESSED_STREAMS_LOG_KEY = 'processed_streams_log/airflow_processed_streams_files.txt'

# --- Redshift Table Configuration ---
REDSHIFT_HOURLY_KPI_STAGING_TABLE = 'hourly_kpis_staging'
REDSHIFT_HOURLY_KPI_FINAL_TABLE = 'hourly_kpis_final'
REDSHIFT_HOURLY_KPI_PRIMARY_KEYS = ['listen_hour']

# Updated Hourly KPI Schema with increased VARCHAR length for top_artists_per_hour
HOURLY_KPI_FINAL_SCHEMA = """
    listen_hour INTEGER PRIMARY KEY,
    top_artists_per_hour VARCHAR(8192), -- Increased length
    unique_listeners INTEGER,
    track_diversity_index NUMERIC(18, 5)
"""

REDSHIFT_GENRE_KPI_STAGING_TABLE = 'genre_kpis_staging'
REDSHIFT_GENRE_KPI_FINAL_TABLE = 'genre_kpis_final'
REDSHIFT_GENRE_KPI_PRIMARY_KEYS = ['track_genre']

# Genre KPI Schema (no change needed here based on current error)
GENRE_KPI_FINAL_SCHEMA = """
    track_genre VARCHAR(256) PRIMARY KEY,
    listen_count INTEGER,
    average_track_duration NUMERIC(18, 5),
    popularity_index NUMERIC(18, 5),
    most_popular_track_per_genre VARCHAR(256)
"""

REDSHIFT_CONN_ID = 'redshift_default'
REDSHIFT_IAM_ROLE_ARN = 'arn:aws:iam::520864643542:role/redshifts3access'


# --- DAG Definition ---

with DAG(
    dag_id='cloud_s3_etl_pipeline',
    start_date= datetime(2025, 5, 17, 20, 15, 0),
    schedule='@hourly',
    catchup=False,
    tags=['cloud', 'etl', 's3', 'streams', 'user', 'song', 'validation', 'transformation', 'kpis', 'redshift', 'upsert']
) as dag:
    # --- Streams Pipeline (Dynamic, listens for new files) ---

    find_new_streams_file_task = PythonOperator(
        task_id='find_new_streams_file',
        python_callable=find_new_streams_file_callable,
        op_kwargs={
            'bucket_name': S3_STREAMS_BUCKET_NAME,
            'prefix': S3_STREAMS_RAW_PREFIX,
            'log_bucket': S3_PROCESSED_LOG_BUCKET,
            'log_key': S3_PROCESSED_STREAMS_LOG_KEY,
        },
    )
    branch_on_new_streams_file = BranchPythonOperator(
        task_id='branch_on_new_streams_file',
        python_callable=decide_streams_pipeline_path,
        provide_context=True,
    )

    dummy_skip_streams_pipeline = DummyOperator(
        task_id='dummy_skip_streams_pipeline',
    )

    ingest_and_stage_streams_data_task = PythonOperator(
        task_id='ingest_and_stage_streams_data_task',
        python_callable=ingest_and_stage_streams_data_from_s3,
        op_kwargs={
            'output_s3_prefix': S3_PREFIX_STREAMS_RAW_STAGED,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
        dag=dag,
    )

    validate_streams_task = PythonOperator(
        task_id='validate_streams_data',
        python_callable=validate_data_task_callable,
        op_kwargs={
            'input_task_id': 'ingest_and_stage_streams_data_task',
            'required_cols': {'user_id', 'track_id', 'listen_time'},
            'critical_null_cols': ['user_id', 'track_id', 'listen_time'],
            'file_name': 'Streams Data',
            'output_s3_prefix': S3_PREFIX_STREAMS_VALIDATED,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    transform_streams_task = PythonOperator(
        task_id='transform_streams_data',
        python_callable=transform_data_task_callable,
        op_kwargs={
            'input_task_id': 'validate_streams_data',
            'file_name': 'Streams Data',
            'output_s3_prefix': S3_PREFIX_STREAMS_TRANSFORMED,
            # Pass this prefix to the callable so it knows where to write the partial KPIs
            'hourly_kpi_output_prefix': S3_PREFIX_HOURLY_KPI,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    # --- Redshift Table Creation Tasks for Hourly KPIs ---
    create_hourly_kpis_final_table = PostgresOperator(
        task_id='create_hourly_kpis_final_table',
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_HOURLY_KPI_FINAL_TABLE} (
                {HOURLY_KPI_FINAL_SCHEMA}
            );
        """,
        dag=dag,
    )

    create_hourly_kpis_staging_table = PostgresOperator(
        task_id='create_hourly_kpis_staging_table',
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_HOURLY_KPI_STAGING_TABLE} (
                {HOURLY_KPI_FINAL_SCHEMA}
            );
        """,
        dag=dag,
    )

    # --- Hourly KPIs Redshift Loading Tasks (using helper function) ---
    load_hourly_kpis_to_redshift_staging_task, upsert_hourly_kpis_to_redshift_final_task, cleanup_hourly_kpis_s3_task = \
        create_redshift_load_and_upsert_tasks(
            dag=dag,
            task_prefix='hourly_kpis',
            s3_bucket=S3_STAGING_BUCKET_NAME,
            # Now pulls the complete hourly KPIs from calculate_genre_kpis_task
            s3_key_upstream_task_id='calculate_genre_kpis',
            s3_key_xcom_key='transformed_hourly_kpis_s3_path',
            redshift_staging_table=REDSHIFT_HOURLY_KPI_STAGING_TABLE,
            redshift_final_table=REDSHIFT_HOURLY_KPI_FINAL_TABLE,
            redshift_primary_keys=REDSHIFT_HOURLY_KPI_PRIMARY_KEYS,
            redshift_conn_id=REDSHIFT_CONN_ID,
            redshift_iam_role_arn=REDSHIFT_IAM_ROLE_ARN,
            # Updated columns for hourly KPIs with new order
            redshift_table_columns=['listen_hour', 'top_artists_per_hour', 'unique_listeners', 'track_diversity_index']
        )

    # --- User & Song Pipelines (Static Data) ---
    ingest_user_data_task = PythonOperator(
        task_id='ingest_user_data',
        python_callable=ingest_and_stage_static_data_from_s3,
        op_kwargs={
            'bucket_name': S3_USER_SONG_BUCKET_NAME,
            's3_key': S3_KEY_USER_RAW,
            'output_s3_prefix': 'raw/users/',
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    validate_user_task = PythonOperator(
        task_id='validate_user_data',
        python_callable=validate_data_task_callable,
        op_kwargs={
            'input_task_id': 'ingest_user_data',
            'required_cols': {'user_id', 'user_name', 'user_age', 'user_country', 'created_at'},
            'critical_null_cols': ['user_id', 'user_name'],
            'file_name': 'User Data',
            'output_s3_prefix': S3_PREFIX_USER_VALIDATED,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    transform_user_task = PythonOperator(
        task_id='transform_user_data',
        python_callable=transform_data_task_callable,
        op_kwargs={
            'input_task_id': 'validate_user_data',
            'file_name': 'User Data',
            'output_s3_prefix': S3_PREFIX_USER_TRANSFORMED,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    ingest_song_data_task = PythonOperator(
        task_id='ingest_song_data',
        python_callable=ingest_and_stage_static_data_from_s3,
        op_kwargs={
            'bucket_name': S3_USER_SONG_BUCKET_NAME,
            's3_key': S3_KEY_SONG_RAW,
            'output_s3_prefix': 'raw/songs/',
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    validate_song_task = PythonOperator(
        task_id='validate_song_data',
        python_callable=validate_data_task_callable,
        op_kwargs={
            'input_task_id': 'ingest_song_data',
            'required_cols': {
                'id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity',
                'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
                'mode', 'speechiness', 'acousticness', 'instrumentalness',
                'liveness', 'valence', 'tempo', 'time_signature', 'track_genre'
            },
            'critical_null_cols': ['id', 'track_id', 'artists', 'album_name', 'track_name'],
            'file_name': 'Song Data',
            'output_s3_prefix': S3_PREFIX_SONG_VALIDATED,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    transform_song_task = PythonOperator(
        task_id='transform_song_data',
        python_callable=transform_data_task_callable,
        op_kwargs={
            'input_task_id': 'validate_song_data',
            'file_name': 'Song Data',
            'output_s3_prefix': S3_PREFIX_SONG_TRANSFORMED,
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
        },
    )

    calculate_genre_kpis_task = PythonOperator(
        task_id='calculate_genre_kpis',
        python_callable=calculate_genre_kpis_callable,
        op_kwargs={
            'S3_STAGING_BUCKET_NAME': S3_STAGING_BUCKET_NAME,
            'S3_PREFIX_GENRE_KPI': S3_PREFIX_GENRE_KPI,
            'S3_PREFIX_HOURLY_KPI': S3_PREFIX_HOURLY_KPI, # This line ensures the argument is passed
        },
   )

    # --- Redshift Table Creation Tasks for Genre KPIs ---
    create_genre_kpis_final_table = PostgresOperator(
        task_id='create_genre_kpis_final_table',
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_GENRE_KPI_FINAL_TABLE} (
                {GENRE_KPI_FINAL_SCHEMA}
            );
        """,
        dag=dag,
    )

    create_genre_kpis_staging_table = PostgresOperator(
        task_id='create_genre_kpis_staging_table',
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_GENRE_KPI_STAGING_TABLE} (
                {GENRE_KPI_FINAL_SCHEMA}
            );
        """,
        dag=dag,
    )

    # --- Genre KPIs Redshift Loading Tasks (using helper function) ---
    load_genre_kpis_to_redshift_staging_task, upsert_genre_kpis_to_redshift_final_task, cleanup_genre_kpis_s3_task = \
        create_redshift_load_and_upsert_tasks(
            dag=dag,
            task_prefix='genre_kpis',
            s3_bucket=S3_STAGING_BUCKET_NAME,
            s3_key_upstream_task_id='calculate_genre_kpis',
            s3_key_xcom_key='genre_kpis_s3_path',
            redshift_staging_table=REDSHIFT_GENRE_KPI_STAGING_TABLE,
            redshift_final_table=REDSHIFT_GENRE_KPI_FINAL_TABLE,
            redshift_primary_keys=REDSHIFT_GENRE_KPI_PRIMARY_KEYS,
            redshift_conn_id=REDSHIFT_CONN_ID,
            redshift_iam_role_arn=REDSHIFT_IAM_ROLE_ARN,
            redshift_table_columns=[
                'track_genre', 'listen_count', 'average_track_duration',
                'popularity_index', 'most_popular_track_per_genre'
            ]
        )

    # --- Task Dependencies ---

    # 1. Ingest, Validate, and Transform ALL data sources first.
    find_new_streams_file_task >> branch_on_new_streams_file
    branch_on_new_streams_file >> [ingest_and_stage_streams_data_task, dummy_skip_streams_pipeline]
    ingest_and_stage_streams_data_task >> validate_streams_task >> transform_streams_task

    ingest_user_data_task >> validate_user_task >> transform_user_task

    ingest_song_data_task >> validate_song_task >> transform_song_task

    # 2. Then, calculate KPIs (which depend on transformed data).
    [transform_streams_task, transform_song_task] >> calculate_genre_kpis_task

    # 3. Finally, create tables in Redshift and load data.

    # Hourly KPI Redshift Load:
    transform_streams_task >> create_hourly_kpis_final_table
    create_hourly_kpis_final_table >> create_hourly_kpis_staging_table
    # IMPORTANT: load_hourly_kpis_to_redshift_staging_task now depends on calculate_genre_kpis_task
    # to get the *completed* hourly KPIs including top_artists_per_hour.
    create_hourly_kpis_staging_table >> load_hourly_kpis_to_redshift_staging_task

    # Genre KPI Redshift Load:
    calculate_genre_kpis_task >> create_genre_kpis_final_table
    create_genre_kpis_final_table >> create_genre_kpis_staging_table
    create_genre_kpis_staging_table >> load_genre_kpis_to_redshift_staging_task
