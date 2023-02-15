from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'Sangwon Lee',
    'start_date': pendulum.now(),
    'retries':3,
    "retry_delay":timedelta(minutes=5),
    'catchup':False,
    'depends_on_past':False
}

dag = DAG('Project5_Data_Pipeline_Automation_with_Airflow',
          default_args=default_args,
          description='Data pipleline automation with Airflow',
          schedule_interval='0 * * * *'
         )

#Get SQL Directory
sql_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),'CreateTable.sql')
#Open a sql file to create table
with open(sql_dir,'r')as f:
    CreateTable = f.read()

start_operator = DummyOperator(task_id='Begin_execution',dag=dag)

#Added extra operator to create tables
create_table = PostgresOperator(
    task_id='Creating_tables',
    postgres_conn_id="redshift",
    sql = CreateTable,
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag = dag,
    aws_credientials_id = 'aws_credentials',
    redS_conn_id = 'redshift',
    table = "staging_events",
    s3_bucket='udacity-dend',
    s3_key = 'log_data',
    json_path = "s3://udacity-dend/log_data/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag = dag,
    aws_credientials_id = 'aws_credentials',
    redS_conn_id = 'redshift',
    table = "staging_songs",
    s3_bucket='udacity-dend',
    s3_key = 'song_data',
    json_path = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag = dag,
    redS_conn_id = 'redshift',
    SQLquery = SqlQueries.songplay_table_insert,        
    table = "songplays",
    Truncate_Ops = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag = dag,
    redS_conn_id = 'redshift',
    SQLquery = SqlQueries.user_table_insert,        
    table = "users",
    Truncate_Ops=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag = dag,
    redS_conn_id = 'redshift',
    SQLquery = SqlQueries.song_table_insert,        
    table = "songs",
    Truncate_Ops = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag = dag,
    redS_conn_id = 'redshift',        
    table = "artists",
    SQLquery = SqlQueries.artist_table_insert,
    Truncate_Ops = False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag = dag,
    redS_conn_id = 'redshift',        
    table = "time",
    SQLquery = SqlQueries.time_table_insert,
    Truncate_Ops=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag = dag,
    redS_conn_id = 'redshift',
    tables=["songplays","users","songs","artists","time"]
)

end_operator = DummyOperator(task_id='End_execution',dag=dag)

start_operator >> create_table
create_table >> [stage_events_to_redshift,stage_songs_to_redshift]
[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]
[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks >> end_operator
run_quality_checks >> end_operator