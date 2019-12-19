from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#
# The following DAG performs the following functions:
#
#       1. Loads songs and events data from S3 to RedShift into their staging tables, repectively
#       2. Transform these tables into fact and dim tables in RedShift
#       3. Performs a data quality check on the fact table in RedShift
#           

default_args = {
    'owner': 'Qing',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
}

dag = DAG('udac_qing_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 1 * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#
# The following code will load events data from S3 to RedShift. Using this s3_key
#       allows as to do some backfilling based on the time (year and month) partition
#       the manifest file makes sure that data is copying correctly
#

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}",
    file_format="JSON",
    manifest_file="s3://udacity-dend/log_json_path.json",
    dag=dag
)

#
# The following code will load songs data from S3 to RedShift. It
#      provides some extra parameters to accelecrate the process
#

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="JSON",
    manifest_file="auto",
    extra_params="compupdate off acceptinvchars",
    dag=dag
)

#
# The following code will load the fact table from the staging tables
#      Note that 

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

#
# The following code will load the dim tables.
#      Note that it allows a 'truncate' function to clean the table before loading
#    

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=True,
    dag=dag
)

#
# Data quality check on the fact table, i.e., songplays
#

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table="songplays",
    columns=['playid', 'start_time', 'userid', 'songid', 'artistid'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator