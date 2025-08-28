import pandas as pd
import os
from datetime import datetime
from airflow.exceptions import AirflowException
from helper_functions import read_s3_csv_to_df, write_df_to_s3_csv # Import from helper_functions.py

def transform_data_task_callable(ti, input_task_id, file_name, output_s3_prefix, S3_STAGING_BUCKET_NAME, hourly_kpi_output_prefix=None):
    """
    Generalized callable for transformation tasks.
    Pulls S3 location from XCom, reads data, applies specific transformations,
    and writes transformed data back to S3.
    """
    print(f"Starting {file_name} data transformation.")
    s3_location_info = ti.xcom_pull(task_ids=input_task_id, key='return_value')

    if not (s3_location_info and 'bucket_name' in s3_location_info and 's3_key' in s3_location_info):
        raise AirflowException(f"No valid S3 location info from task '{input_task_id}' found in XCom.")

    input_bucket_name = s3_location_info['bucket_name']
    input_s3_key = s3_location_info['s3_key']

    print(f"Attempting to read {file_name} for transformation from s3://{input_bucket_name}/{input_s3_key}")
    try:
        df = read_s3_csv_to_df(input_bucket_name, input_s3_key)
        print(f"Successfully read {len(df)} rows for {file_name} transformation.")
    except Exception as e:
        raise AirflowException(f"Failed to read {file_name} for transformation from S3: {e}")

    # Apply specific transformations based on file_name
    if file_name == "Streams Data":
        # 1. Convert 'listen_time' to datetime and drop unparseable rows
        if 'listen_time' in df.columns:
            df['listen_time'] = pd.to_datetime(df['listen_time'], errors='coerce')
            df.dropna(subset=['listen_time'], inplace=True)
            print("Converted 'listen_time' to datetime format for Streams.")
        # 2. Ensure user_id and track_id are strings
        if 'user_id' in df.columns:
            df['user_id'] = df['user_id'].astype(str)
        if 'track_id' in df.columns:
            df['track_id'] = df['track_id'].astype(str)

        # --- Hourly KPIs Calculation for Streams Data (Partial - without top_artists_per_hour here) ---
        if 'listen_time' in df.columns and 'user_id' in df.columns and 'track_id' in df.columns:
            df['listen_hour'] = df['listen_time'].dt.hour
            print("Extracted 'listen_hour' for Streams.")

            # Unique Listeners: The distinct number of users streaming music in a given hour
            hourly_unique_listeners = df.groupby('listen_hour')['user_id'].nunique().reset_index(name='unique_listeners')
            print("Calculated Unique Listeners per Hour.")
            
            # Track Diversity Index: A measure of how varied the tracks played in an hour are, based on the number of unique tracks played compared to total plays.
            hourly_track_diversity = df.groupby('listen_hour').agg(
                unique_tracks=('track_id', 'nunique'),
                total_plays=('track_id', 'count')
            ).reset_index()
            hourly_track_diversity['track_diversity_index'] = hourly_track_diversity['unique_tracks'] / hourly_track_diversity['total_plays']
            hourly_track_diversity['track_diversity_index'].fillna(0, inplace=True) # Handle division by zero
            print("Calculated Track Diversity Index per Hour.")

            # Prepare partial hourly KPIs DataFrame
            hourly_kpis_partial_df = pd.merge(hourly_unique_listeners, hourly_track_diversity[['listen_hour', 'track_diversity_index']], on='listen_hour', how='left')
            
            # Ensure numeric columns are properly filled with 0 and cast to final types
            hourly_kpis_partial_df['unique_listeners'] = hourly_kpis_partial_df['unique_listeners'].fillna(0).astype(int)
            hourly_kpis_partial_df['track_diversity_index'] = hourly_kpis_partial_df['track_diversity_index'].fillna(0).astype(float)

            # Write Partial Hourly KPIs to S3
            if hourly_kpi_output_prefix:
                current_time_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                # Use a specific key for the partial data, it will be completed in calculate_genre_kpis_callable
                hourly_kpi_partial_output_key = f"{hourly_kpi_output_prefix}hourly_kpis_partial_{current_time_str}.csv"
                write_df_to_s3_csv(hourly_kpis_partial_df, S3_STAGING_BUCKET_NAME, hourly_kpi_partial_output_key)
                # Push this partial path to XCom, it will be pulled by calculate_genre_kpis_callable
                ti.xcom_push(key='transformed_hourly_kpis_partial_s3_path', value={
                    'bucket_name': S3_STAGING_BUCKET_NAME,
                    's3_key': hourly_kpi_partial_output_key
                })
                print(f"Pushed Partial Hourly KPIs to S3: s3://{S3_STAGING_BUCKET_NAME}/{hourly_kpi_partial_output_key}")
            else:
                print("Hourly KPI output prefix not provided, skipping partial hourly KPI staging to S3.")

    elif file_name == "User Data":
        # 1. Convert 'user_age' to integer, handling errors
        if 'user_age' in df.columns:
            df['user_age'] = pd.to_numeric(df['user_age'], errors='coerce').astype('Int64')
            print("Converted 'user_age' to numeric type for User Data.")
        # 2. Convert 'created_at' to datetime and drop unparseable rows
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df.dropna(subset=['created_at'], inplace=True)
            print("Converted 'created_at' to datetime format for User Data.")
        # 3. Basic cleaning for 'user_name' and 'user_country'
        if 'user_name' in df.columns:
            df['user_name'] = df['user_name'].astype(str).str.strip()
        if 'user_country' in df.columns:
            df['user_country'] = df['user_country'].astype(str).str.strip().str.upper()

    elif file_name == "Song Data":
        # Convert relevant columns to numeric
        numeric_cols = [
            'popularity', 'duration_ms', 'danceability', 'energy', 'key',
            'loudness', 'speechiness', 'acousticness', 'instrumentalness',
            'liveness', 'valence', 'tempo', 'time_signature'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                print(f"Converted '{col}' to numeric type for Song Data.")
        # 1. Convert 'explicit' to boolean
        if 'explicit' in df.columns:
            df['explicit'] = df['explicit'].astype(str).str.lower().map({'true': True, '1': True, 'false': False, '0': False}).fillna(False)
            print("Converted 'explicit' to boolean for Song Data.")
        # 2. Clean string columns
        string_cols = ['artists', 'album_name', 'track_name', 'track_genre']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Drop rows with missing values in critical song columns
        critical_song_cols_for_dropna = ['id', 'track_id', 'artists', 'album_name', 'track_name']
        initial_rows = len(df)
        df.dropna(subset=critical_song_cols_for_dropna, inplace=True)
        dropped_rows = initial_rows - len(df)
        if dropped_rows > 0:
            print(f"Dropped {dropped_rows} rows from Song Data due to missing values in critical columns: {critical_song_cols_for_dropna}")
        else:
            print("No rows dropped from Song Data due to missing values in critical columns.")

    else:
        print(f"[WARNING] No specific transformations defined for {file_name}. Skipping transformation.")


    print(f"{file_name} transformation complete. Writing transformed data to S3.")
    current_time_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_file_name, file_extension = os.path.splitext(input_s3_key.split('/')[-1])
    output_s3_key = f"{output_s3_prefix}{base_file_name}_{current_time_str}{file_extension}"
    write_df_to_s3_csv(df, S3_STAGING_BUCKET_NAME, output_s3_key)

    transformed_s3_location_info = {
        'bucket_name': S3_STAGING_BUCKET_NAME,
        's3_key': output_s3_key
    }

    print(f"Pushed transformed {file_name} S3 path to XCom: s3://{S3_STAGING_BUCKET_NAME}/{output_s3_key}")
    return transformed_s3_location_info


def calculate_genre_kpis_callable(ti, S3_STAGING_BUCKET_NAME, S3_PREFIX_GENRE_KPI, S3_PREFIX_HOURLY_KPI):
    """
    Calculates genre-level KPIs and completes hourly KPIs by joining transformed streams and song data.
    """
    print("Starting genre-level and final hourly KPI calculation.")

    transformed_streams_s3_info = ti.xcom_pull(task_ids='transform_streams_data', key='return_value')
    transformed_song_s3_info = ti.xcom_pull(task_ids='transform_song_data', key='return_value')
    # Pull the partial hourly KPIs calculated in transform_streams_data
    hourly_kpis_partial_s3_info = ti.xcom_pull(task_ids='transform_streams_data', key='transformed_hourly_kpis_partial_s3_path')


    if not (transformed_streams_s3_info and transformed_song_s3_info and hourly_kpis_partial_s3_info):
        print("[INFO] One or more transformed data paths are missing (likely streams data was skipped). "
              "KPIs might be incomplete if streams data is crucial.")
        # Create empty DataFrames for both KPIs to avoid downstream errors
        genre_kpis_df = pd.DataFrame(columns=[
            'track_genre', 'listen_count', 'average_track_duration',
            'popularity_index', 'most_popular_track_per_genre'
        ])
        hourly_kpis_final_df = pd.DataFrame(columns=[
            'listen_hour', 'top_artists_per_hour', 'unique_listeners', 'track_diversity_index'
        ])

        print("Returning empty DataFrames for KPIs due to missing upstream data.")
        # Write empty genre KPIs
        genre_output_s3_key = f"{S3_PREFIX_GENRE_KPI}genre_kpis_empty_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        write_df_to_s3_csv(genre_kpis_df, S3_STAGING_BUCKET_NAME, genre_output_s3_key)
        ti.xcom_push(key='genre_kpis_s3_path', value={'bucket_name': S3_STAGING_BUCKET_NAME, 's3_key': genre_output_s3_key})

        # Write empty hourly KPIs
        hourly_output_s3_key = f"{S3_PREFIX_HOURLY_KPI}hourly_kpis_empty_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        write_df_to_s3_csv(hourly_kpis_final_df, S3_STAGING_BUCKET_NAME, hourly_output_s3_key)
        ti.xcom_push(key='transformed_hourly_kpis_s3_path', value={'bucket_name': S3_STAGING_BUCKET_NAME, 's3_key': hourly_output_s3_key})
        return # Exit if data is missing

    streams_df = read_s3_csv_to_df(transformed_streams_s3_info['bucket_name'], transformed_streams_s3_info['s3_key'])
    songs_df = read_s3_csv_to_df(transformed_song_s3_info['bucket_name'], transformed_song_s3_info['s3_key'])
    hourly_kpis_final_df = read_s3_csv_to_df(hourly_kpis_partial_s3_info['bucket_name'], hourly_kpis_partial_s3_info['s3_key'])

    print(f"Read {len(streams_df)} rows from transformed streams, {len(songs_df)} rows from transformed songs, and {len(hourly_kpis_final_df)} rows from partial hourly KPIs.")

    # Ensure track_id and track_genre exist and are consistent types for joining
    if 'track_id' not in streams_df.columns or 'track_id' not in songs_df.columns:
        raise AirflowException("Missing 'track_id' in transformed streams or songs data for KPI join.")
    if 'track_genre' not in songs_df.columns:
        print("[WARNING] Missing 'track_genre' in transformed songs data for genre KPI calculation.")
    if 'popularity' not in songs_df.columns or 'duration_ms' not in songs_df.columns:
        print("[WARNING] Missing 'popularity' or 'duration_ms' in songs data for some KPI calculations.")
    if 'artists' not in songs_df.columns:
        print("[WARNING] Missing 'artists' in songs data for 'Top Artists per Hour' calculation.")


    streams_df['track_id'] = streams_df['track_id'].astype(str)
    songs_df['track_id'] = songs_df['track_id'].astype(str)

    # --- Calculation for Top Artists per Hour (using user-provided logic) ---
    # Step 1: Merge streams and songs to get artist information
    # Ensure 'listen_time' is still in streams_df before merging
    if 'listen_time' not in streams_df.columns:
        raise AirflowException("Missing 'listen_time' in streams data for hourly KPI calculations.")

    # Rename 'listen_time' in streams_df to avoid suffix issues in merge if 'listen_time' exists in songs_df
    # For now, let's assume it only exists in streams_df based on previous transformation.
    merged_df_for_artists = pd.merge(streams_df, songs_df[['track_id', 'artists']], on='track_id', how='left')

    # Step 2: Extract listen hour (if not already floor to hour from streams_df processing)
    # The 'listen_hour' column from streams_df already exists and is floored to hour in transform_data_task_callable
    # so we don't need to re-floor it here. Just ensure it's present.
    if 'listen_hour' not in merged_df_for_artists.columns:
        merged_df_for_artists['listen_time'] = pd.to_datetime(merged_df_for_artists['listen_time'])
        merged_df_for_artists['listen_hour'] = merged_df_for_artists['listen_time'].dt.floor('H') # Round down to the hour


    # Step 3: Split artists by ';'
    # Handle cases where 'artists' might be NaN or not a string
    merged_df_for_artists['artists_list'] = merged_df_for_artists['artists'].astype(str).str.split(';')

    # Step 4: Explode artists to count each artist separately
    merged_exploded = merged_df_for_artists.explode('artists_list')

    # Optional: Strip whitespace
    merged_exploded['artists_list'] = merged_exploded['artists_list'].str.strip()

    # Filter out empty strings or NaN artists if any result from splitting/stripping
    merged_exploded = merged_exploded[merged_exploded['artists_list'] != '']
    merged_exploded = merged_exploded.dropna(subset=['artists_list'])


    # Step 5: Count plays per hour per artist
    # Ensure 'artists_list' is present after filtering
    if 'artists_list' in merged_exploded.columns:
        artist_counts = merged_exploded.groupby(['listen_hour', 'artists_list']).size().reset_index(name='play_count')
    else:
        print("[WARNING] 'artists_list' not found after processing for top artists. Skipping top artists calculation.")
        artist_counts = pd.DataFrame(columns=['listen_hour', 'artists_list', 'play_count']) # Empty DataFrame


    top_artists_per_hour_df = pd.DataFrame(columns=['listen_hour', 'top_artists_per_hour'])

    if not artist_counts.empty:
        # Step 6: Find max play count per hour
        artist_counts['max_play_count'] = artist_counts.groupby('listen_hour')['play_count'].transform('max')

        # Step 7: Filter top artist(s) per hour
        top_artists = artist_counts[artist_counts['play_count'] == artist_counts['max_play_count']]

        # Step 8: Aggregate multiple top artists per hour (comma separated)
        top_artists_per_hour_df = top_artists.groupby('listen_hour')['artists_list'].apply(lambda x: ', '.join(sorted(x.unique()))).reset_index(name='top_artists_per_hour')
    else:
        print("[INFO] No artist play counts found, 'top_artists_per_hour' will be empty or N/A.")

    # Merge top_artists_per_hour_df with the partial hourly_kpis_final_df
    hourly_kpis_final_df = pd.merge(hourly_kpis_final_df, top_artists_per_hour_df, on='listen_hour', how='left')

    # Ensure final hourly KPIs are in the correct order and types
    hourly_kpis_final_df['top_artists_per_hour'] = hourly_kpis_final_df['top_artists_per_hour'].fillna('N/A').astype(str)
    hourly_kpis_final_df['unique_listeners'] = hourly_kpis_final_df['unique_listeners'].fillna(0).astype(int)
    hourly_kpis_final_df['track_diversity_index'] = hourly_kpis_final_df['track_diversity_index'].fillna(0).astype(float)
    
    # Reorder columns as requested: listen_hour (PK), top_artists_per_hour, unique_listeners, track_diversity_index
    hourly_kpis_final_df = hourly_kpis_final_df[[
        'listen_hour', 'top_artists_per_hour', 'unique_listeners', 'track_diversity_index'
    ]]

    print("Completed final hourly KPI calculation.")

    # Write Final Hourly KPIs to S3
    hourly_output_s3_key = f"{S3_PREFIX_HOURLY_KPI}hourly_kpis_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    write_df_to_s3_csv(hourly_kpis_final_df, S3_STAGING_BUCKET_NAME, hourly_output_s3_key)
    ti.xcom_push(key='transformed_hourly_kpis_s3_path', value={
        'bucket_name': S3_STAGING_BUCKET_NAME,
        's3_key': hourly_output_s3_key
    })
    print(f"Pushed Final Hourly KPIs to S3: s3://{S3_STAGING_BUCKET_NAME}/{hourly_output_s3_key}")


    # --- Genre KPIs Calculation (existing logic, ensured to be correct) ---
    # Merge streams and songs data for genre KPIs. Use inner join for KPIs that require both stream and song metadata
    # Use streams_df as the base for merging, ensuring 'track_id' is present
    merged_df_for_genre = pd.merge(streams_df, songs_df, on='track_id', how='inner', suffixes=('_stream', '_song'))
    print(f"Merged streams and songs data for genre KPIs, resulting in {len(merged_df_for_genre)} rows.")

    if merged_df_for_genre.empty:
        print("[INFO] Merged DataFrame for genre KPIs is empty, no genre KPIs can be calculated.")
        genre_kpis_df = pd.DataFrame(columns=[
            'track_genre', 'listen_count', 'average_track_duration',
            'popularity_index', 'most_popular_track_per_genre'
        ])
    else:
        # Listen Count: Total number of times tracks in a genre have been played.
        listen_count_per_genre = merged_df_for_genre.groupby('track_genre').size().reset_index(name='listen_count')

        # Average Track Duration: The mean duration of all tracks within a genre.
        songs_df['duration_ms'] = pd.to_numeric(songs_df['duration_ms'], errors='coerce')
        avg_duration_per_genre = songs_df.dropna(subset=['duration_ms']).groupby('track_genre')['duration_ms'].mean().reset_index(name='average_track_duration')

        # Popularity Index: (Average Popularity of tracks in genre * Total Listen Count of genre)
        genre_popularity_metrics = merged_df_for_genre.groupby('track_genre').agg(
            total_genre_listens=('track_id', 'count')
        ).reset_index()

        avg_song_popularity = songs_df.groupby('track_genre')['popularity'].mean().reset_index(name='avg_song_popularity')

        genre_popularity_metrics = pd.merge(genre_popularity_metrics, avg_song_popularity, on='track_genre', how='left')
        genre_popularity_metrics['popularity_index'] = genre_popularity_metrics['avg_song_popularity'] * genre_popularity_metrics['total_genre_listens']
        genre_popularity_metrics['popularity_index'].fillna(0, inplace=True)

        # Most Popular Track per Genre: The track with the highest engagement (based on popularity)
        most_popular_tracks_per_genre_df = pd.DataFrame()
        if 'popularity' in songs_df.columns:
            most_popular_tracks_per_genre_df = songs_df.loc[songs_df.groupby('track_genre')['popularity'].idxmax()]
            most_popular_tracks_per_genre_df = most_popular_tracks_per_genre_df[['track_genre', 'track_name']].rename(
                columns={'track_name': 'most_popular_track_per_genre'}
            )
            print("Calculated Most Popular Track per Genre.")
        else:
            print("[WARNING] 'popularity' column not found in songs data, cannot determine Most Popular Track per Genre effectively.")

        # Aggregate all genre KPIs into a single DataFrame
        genre_kpis_df = listen_count_per_genre
        genre_kpis_df = pd.merge(genre_kpis_df, avg_duration_per_genre, on='track_genre', how='left')
        genre_kpis_df = pd.merge(genre_kpis_df, genre_popularity_metrics[['track_genre', 'popularity_index']], on='track_genre', how='left')
        genre_kpis_df = pd.merge(genre_kpis_df, most_popular_tracks_per_genre_df, on='track_genre', how='left')

        # Fill any remaining NaNs from merges and explicitly cast to appropriate types
        genre_kpis_df.fillna({
            'listen_count': 0,
            'average_track_duration': 0.0,
            'popularity_index': 0.0
        }, inplace=True)
        genre_kpis_df['most_popular_track_per_genre'] = genre_kpis_df['most_popular_track_per_genre'].fillna('N/A').astype(str)

        genre_kpis_df['listen_count'] = genre_kpis_df['listen_count'].astype(int)
        genre_kpis_df['average_track_duration'] = genre_kpis_df['average_track_duration'].astype(float)
        genre_kpis_df['popularity_index'] = genre_kpis_df['popularity_index'].astype(float)
        
        # Reorder columns as requested
        genre_kpis_df = genre_kpis_df[[
            'track_genre', 'listen_count', 'average_track_duration',
            'popularity_index', 'most_popular_track_per_genre'
        ]]

    print("Genre-level KPI calculation complete. Writing KPIs to S3.")
    genre_output_s3_key = f"{S3_PREFIX_GENRE_KPI}genre_kpis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    write_df_to_s3_csv(genre_kpis_df, S3_STAGING_BUCKET_NAME, genre_output_s3_key)

    ti.xcom_push(key='genre_kpis_s3_path', value={
        'bucket_name': S3_STAGING_BUCKET_NAME,
        's3_key': genre_output_s3_key
    })
    print(f"Pushed Genre KPIs S3 path to XCom: s3://{S3_STAGING_BUCKET_NAME}/{genre_output_s3_key}")

    # The function now pushes two XComs: one for hourly and one for genre KPIs.
    # It does not return a single dictionary, as it handles multiple outputs.
