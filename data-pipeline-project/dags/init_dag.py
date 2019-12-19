from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

#
# The following DAG performs the following functions:
#
#       1. Create all the RedShift tables, including both staging tables and fact/dim tables
#       2. Performs a data quality check to make sure that they are created properly
#      

default_args = {
    'owner': 'Qing',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': datetime(2019, 12, 19),
}

dag = DAG('init_dag',
          default_args=default_args,
          description='Create all the tables in Redshift',
          max_active_runs=3,
          schedule_interval='@daily',
        )

#
# The following code execute the sql queries from the create_table.sql to
#       create all the tables
#

create_redshift_tables = PostgresOperator(
    task_id='create_redshift_tables',
    postgres_conn_id="redshift",
    sql='sql/create_tables.sql',
    autocommit=True,
    dag=dag
)

#
# The following code performs a quality check to make sure 
#       all tables are created
#

def check_table_exists(*args, **kwargs):
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        select true where EXISTS (SELECT *
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME IN ('staging_songs', 'staging_events', 'artists', 'songs', 'users', 'time', 'songplays'))
    """)
    if not records[0][0]:
        raise ValueError(f"Failed to create all the tables")

check_redshift_tables = PythonOperator(
    task_id='check_redshift_tables',
    python_callable=check_table_exists,
    dag=dag
)

#
# Task ordering for the DAG tasks 
#

create_redshift_tables >> check_redshift_tables
