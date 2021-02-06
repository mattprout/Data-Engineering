from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#bucket_name = 'dataengineering-nano-dwh-bucket'
bucket_name = 'udacity-dend'

#
# Default arguments for the DAG
#     - Retry 3 times
#     - Does not have dependencies on past runs
#     - Retry every 5 minutes
#     - Do not send emails on retries
#     - Catchup is turned off
#
default_args = {
    'owner': 'Sparkify',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

#
# Define the DAG.
#     - Scheduled to run every hour
#
dag = DAG('Sparkify_ETL_Workflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket_name,
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    json_args="'s3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket_name,
    s3_key="song_data/",
    json_args="'auto ignorecase'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    clear=True,
    table = "users",
    query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    clear=True,
    table = "songs",
    query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    clear=True,
    table = "artists",
    query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    clear=True,
    table = "time",
    query = SqlQueries.time_table_insert
)

#
# Define quality checks to run, and their expected results
#
dq_checks=[
    {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    dq_checks = dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
