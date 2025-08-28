# ETL pipeline with Airflow

A music streaming service requires an end-to-end data pipeline to analyze user streaming behavior. The data pipeline integrates data from multiple sources, processes it, and generate key performance indicators (KPIs) for business intelligence.
The streaming data is being stored in **Amazon S3** and the user and song metadata in an RDS. 
- Apache Airflow handles the orchestration of the pipeline where the streaming data(song data) that hits the s3 is ingested and the user and song metadata in the RDS is ingested and carried further downstream. 
- Amazon Redshift is implemented here for data warehousing 


## 1. Overall Pipeline Purpose

Imagine you have a music streaming service. Every time someone listens to a song, that event is recorded. You also have static information about your users (their age, country) and the songs themselves (genre, artist, popularity).

This pipeline's main goal is to:

* Collect all this **raw data** from various sources whiich is the S3 bucket and the Amazon RDS.
* **Clean and prepare** the data to ensure it's accurate and consistent.
* Calculate important **metrics (KPIs)** like how many unique users are listening per hour, which artists are most popular, or the listening trends for different music genres.
* Load these prepared and analyzed insights into a powerful database (**Redshift**) where they can be quickly queried for reporting and business decisions.


## 2. Deep Dive into the Python Scripts

The pipeline is built using several modular Python scripts. Each script has a specific role:

### 2.1. `helper_functions.py`

This script contains reusable utility functions that make it easier to interact with Amazon S3. 

* **`read_s3_csv_to_df(bucket_name, s3_key, aws_conn_id)`**:
    * **Purpose**: Reads a CSV (Comma Separated Values) file directly from an S3 bucket and converts it into a Pandas DataFrame. 
    * **How it works**: It uses Airflow's `S3Hook` to connect to S3 and fetch the file content, then Pandas to parse the CSV.
* **`write_df_to_s3_csv(df, bucket_name, s3_key, aws_conn_id)`**:
    * **Purpose**: Takes a Pandas DataFrame and writes its contents back to an S3 bucket as a CSV file.
    * **How it works**: It converts the DataFrame into a CSV string in memory and then uses `S3Hook` to upload that string to S3.

### 2.2. `extraction.py`

This script is responsible for the "**Extract**" part of ETL. It pulls raw data from your source S3 buckets and stages it (puts it into a temporary location) in another S3 bucket for further processing.

* **`get_processed_files_s3(log_bucket, log_key)`** and **`add_processed_file_s3(log_bucket, log_key, s3_key_to_add)`**:
    * **Purpose**: These functions help the pipeline track which streaming data files have already been processed to avoid reprocessing the same data repeatedly. They manage a log file in S3.
* **`find_new_streams_file_callable(bucket_name, prefix, log_bucket, log_key, ti)`**:
    * **Purpose**: This is a key function for stream data. It scans a specific S3 location (`streams/`) to find new CSV files that haven't been processed yet.
    * **How it works**: It compares the current files in S3 with its internal log of already processed files. If a new file is found, its S3 location is "pushed" to Airflow's XCom (a mechanism for tasks to communicate) so the next task knows which file to process. If no new files are found, it signals to skip the stream processing.
* **`decide_streams_pipeline_path(ti)`**:
    * **Purpose**: This is a branching function. Based on whether `find_new_streams_file_callable` found a new file, this function tells Airflow which path to take in the pipeline: either proceed with processing the new stream file or skip to a "dummy" task (a placeholder indicating no action is needed).
* **`ingest_and_stage_streams_data_from_s3(output_s3_prefix, ti, S3_STAGING_BUCKET_NAME)`**:
    * **Purpose**: Reads the newly found stream data file from its source S3 location and copies it to a designated "raw" staging area within the `S3_STAGING_BUCKET_NAME`.
    * **How it works**: It pulls the S3 path of the new file from XCom (which `find_new_streams_file_callable` pushed), uses `read_s3_csv_to_df`, and then `write_df_to_s3_csv` to move the data.
* **`ingest_and_stage_static_data_from_s3(bucket_name, s3_key, output_s3_prefix, ti, S3_STAGING_BUCKET_NAME)`**:
    * **Purpose**: Similar to the streams ingestion, but for static data files like user and song information. These files are typically updated less frequently or at all.
    * **How it works**: It directly reads a specified S3 file and stages it in the raw area.

### 2.3. `validation.py`

This script performs the "**Validate**" step, ensuring the quality and correctness of the data after it has been ingested. It's crucial for catching issues early.

* **`validate_columns_logic(df, required_columns, file_name)`**:
    * **Purpose**: Checks if all expected columns are present in a DataFrame. This prevents downstream errors if a source file suddenly changes its format.
* **`check_null_values(df, columns_to_check, file_name)`**:
    * **Purpose**: Identifies if critical columns have missing (null) values. It logs warnings but doesn't necessarily fail the task, as some nulls might be handled in transformation.
* **`validate_data_task_callable(ti, input_task_id, required_cols, critical_null_cols, file_name, output_s3_prefix, S3_STAGING_BUCKET_NAME)`**:
    * **Purpose**: A general function that orchestrates the validation process for any given dataset (streams, users, or songs).
    * **How it works**: It pulls the S3 path of the data from XCom, reads it, applies the column and null checks, and then writes the validated data to another S3 staging area (`validated/`).

### 2.4. `transformation.py`

This script handles the "**Transform**" part of ETL. It cleans, reshapes, and aggregates data to derive meaningful insights (KPIs).

* **`transform_data_task_callable(ti, input_task_id, file_name, output_s3_prefix, S3_STAGING_BUCKET_NAME, hourly_kpi_output_prefix=None)`**:
    * **Purpose**: Applies specific transformations based on the type of data (streams, users, or songs).
    * **Streams Data Transformations**:
        * Converts `listen_time` to proper datetime format.
        * Calculates `unique_listeners` and `track_diversity_index` (unique tracks / total plays) per hour. These are written to a partial hourly KPI file in S3.
    * **User Data Transformations**: Converts `user_age` to numeric and `created_at` to datetime.
    * **Song Data Transformations**: Converts numeric columns to correct types, 'explicit' to boolean, and cleans up string fields. It also drops rows with missing critical song data.
    * **How it works**: It reads the validated data from S3, applies the transformations, and writes the transformed data to yet another S3 staging area (`transformed/`).
* **`calculate_genre_kpis_callable(ti, S3_STAGING_BUCKET_NAME, S3_PREFIX_GENRE_KPI, S3_PREFIX_HOURLY_KPI)`**:
    * **Purpose**: This is the central calculation hub. It combines the transformed streams data and transformed song data to compute both the final hourly KPIs and all genre-level KPIs.
    * **How it works**:
        * It reads the transformed streams data, transformed song data, and the partial hourly KPIs (from `transform_streams_task`) from S3.
        * **Hourly KPIs (Completes)**: It uses the provided logic to calculate `top_artists_per_hour` by merging streams with song artist information, splitting artists, counting plays, and finding the top artists per hour. It then merges this with the previously calculated `unique_listeners` and `track_diversity_index` to form the final hourly KPIs.
        * **Genre KPIs**:
            * **Listen Count**: Counts total plays per genre.
            * **Average Track Duration**: Calculates the average song duration for tracks within each genre.
            * **Popularity Index**: A calculated score based on average song popularity and total listens within a genre.
            * **Most Popular Track per Genre**: Identifies the song with the highest popularity within each genre.
        * It writes both the final hourly KPIs and the genre KPIs as separate CSVs to their respective S3 `transformed/` prefixes. It also pushes their S3 paths to XCom for the loading tasks.

### 2.5. `load.py`

This script handles the "**Load**" part of ETL. It moves the transformed data and KPIs from the S3 into the Redshift data warehouse.

* **`_execute_redshift_sql_callable(sql_commands, redshift_conn_id)`**:
    * **Purpose**: A utility function to run any SQL command (like `CREATE TABLE`, `DELETE`, `INSERT`) against your Redshift cluster.
* **`_copy_s3_to_redshift_callable(ti, s3_key_upstream_task_id, s3_key_xcom_key, s3_fallback_bucket, redshift_table, redshift_iam_role_arn, redshift_conn_id, copy_options, target_columns)`**:
    * **Purpose**: Performs the core data loading from S3 to Redshift. Redshift has a very efficient `COPY` command for this.
    * **How it works**: It pulls the S3 path of the transformed data from XCom, constructs a `COPY` SQL command (specifying the S3 bucket, key, IAM role for access, and column mapping), and executes it. Data is first loaded into a temporary "staging" table in Redshift.
* **`_delete_s3_key_callable(ti, s3_key_upstream_task_id, s3_key_xcom_key, aws_conn_id)`**:
    * **Purpose**: Cleans up temporary files in S3 after they have been successfully loaded into Redshift. This is important for cost management and keeping S3 organized.
* **`create_redshift_load_and_upsert_tasks(...)`**:
    * **Purpose**: This is a helper function that generates a set of Airflow tasks for a given Redshift table (e.g., for hourly KPIs or genre KPIs). It encapsulates the common pattern of:
        * Loading data from S3 into a staging table in Redshift.
        * Performing an **UPSERT** operation from the staging table to the final table.
        * **UPSERT (Update or Insert)**: This is a database operation where, for new data, it inserts the records. For existing records (identified by primary keys), it updates them. This prevents duplicate data and keeps your final tables current. The SQL generated by this function first `DELETE`s old matching records from the final table, then `INSERT`s all records from the staging table, and finally `TRUNCATE`s (empties) the staging table.
        * Cleaning up the S3 source file.

### 2.6. `etl_dag.py` (The Orchestrator)

This is the main Airflow DAG (Directed Acyclic Graph) file. It defines the entire pipeline, specifying the order of operations, what each step does, and how they pass information. It's the "brain" that tells Airflow how to run your ETL process.

* **S3 Configuration**: Defines all the S3 bucket names and prefixes used across the pipeline for raw, staged, validated, and transformed data.
* **Redshift Table Configuration**: Defines the names and schemas (column names and data types) for your final and staging tables in Redshift for both hourly and genre KPIs. This is critical for matching data types and ensuring data integrity.
    * **`HOURLY_KPI_FINAL_SCHEMA`**: Defines `listen_hour`, `top_artists_per_hour`, `unique_listeners`, `track_diversity_index`.
    * **`GENRE_KPI_FINAL_SCHEMA`**: Defines `track_genre`, `listen_count`, `average_track_duration`, `popularity_index`, `most_popular_track_per_genre`.
* **DAG Definition (`cloud_s3_etl_pipeline`)**:
    * Sets the start date and schedule (hourly).
    * **Tasks**: Each step in the pipeline is defined as an Airflow `PythonOperator` (running a Python function) or `PostgresOperator` (running SQL).
    * **Task Dependencies (`>>`)**: This is where the workflow is defined. For example:
        * `ingest_and_stage_streams_data_task >> validate_streams_task >> transform_streams_task` means ingestion must complete before validation, which must complete before transformation.
        * `[transform_streams_task, transform_song_task] >> calculate_genre_kpis_task` means both stream and song transformations must be done before KPI calculation can start.
        * Crucially, the loading tasks now depend on `calculate_genre_kpis_task` because that's where the final, complete KPIs (including `top_artists_per_hour`) are produced.
    * **XComs (Cross-Communication)**: Tasks use XComs to pass small pieces of information (like the S3 path of a processed file) to subsequent tasks. This ensures that each task knows where to find the data it needs.

## 3. Workflow of the Pipeline

Here's a step-by-step walkthrough of how data moves through the ETL pipeline:

1.  **Find New Streams**: The pipeline first checks the `lab1-etl-landingzone/streams/` S3 bucket for any new streaming data files that haven't been processed yet.
    * **If new files are found**: The pipeline proceeds with the streams data path. The S3 path of the newest file is logged and passed on.
    * **If no new files**: The streams-specific part of the pipeline is skipped for this run.
2.  **Ingest and Stage Raw Data**:
    * **Streams Data**: If a new streams file was found, it's read from the source S3 bucket and copied to `staging-bucket-lab1test/raw/streams/`.
    * **Static Data (Users & Songs)**: Separately, the `users_data.csv` and `songs_data.csv` files are read from `etl-lab1-rds-landingzone/processed/` and copied to `staging-bucket-lab1test/raw/users/` and `staging-bucket-lab1test/raw/songs/` respectively.
3.  **Validate Data**:
    * Each staged raw dataset (streams, users, songs) undergoes validation.
    * It checks for required columns and logs warnings for null values in critical columns.
    * Validated data is then moved to `staging-bucket-lab1test/validated/` prefixes (e.g., `validated/streams/`).
4.  **Transform Data**:
    * Each validated dataset undergoes specific transformations:
        * **Streams**: `listen_time` is parsed, and `unique_listeners` and `track_diversity_index` are calculated per hour. This partial hourly KPI data is saved to S3.
        * **Users**: Data types for age and creation time are adjusted.
        * **Songs**: Numeric fields are cast, boolean explicit is converted, and text fields are cleaned.
    * Transformed data is saved to `staging-bucket-lab1test/transformed/` prefixes (e.g., `transformed/streams/`).
5.  **Calculate KPIs (Central Hub)**:
    * The `calculate_genre_kpis_task` runs after both the streams and song data have been transformed.
    * It pulls the transformed streams and songs data from S3, along with the partial hourly KPIs from `transform_streams_task`.
    * It then calculates:
        * The final, complete hourly KPIs, including `top_artists_per_hour` (which requires both streams and song data). These are written to `transformed/hourly_kpis/`.
        * All the genre-level KPIs (`listen_count`, `average_track_duration`, `popularity_index`, `most_popular_track_per_genre`). These are written to `transformed/genre_kpis/`.
6.  **Create Redshift Tables (If Not Exists)**:
    * Before loading, Airflow tasks (`create_hourly_kpis_final_table`, `create_hourly_kpis_staging_table`, `create_genre_kpis_final_table`, `create_genre_kpis_staging_table`) ensure that the necessary final and staging tables in Redshift exist with the correct schema.
7.  **Load Data to Redshift**:
    * For both hourly and genre KPIs:
        * Data is loaded from their respective `transformed/` S3 paths into their Redshift staging tables using the efficient `COPY` command.
        * An **UPSERT** operation is performed: new records from the staging table are inserted into the final table, and existing records (based on primary keys) are updated.
        * The Redshift staging table is truncated (emptied) to prepare for the next run.
        * The temporary CSV file in S3 that was just loaded is deleted to keep S3 clean.

