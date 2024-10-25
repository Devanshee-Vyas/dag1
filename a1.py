

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False
}

# Define the DAG
with DAG('simplified_snowflake_etl_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Task 1: Set Stage - Create tables and set the stage
    set_stage = SnowflakeOperator(
        task_id='set_stage',
        sql="""
            -- Create user_session_channel and session_timestamp tables if not exists
            CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'
            );

            CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp
            );

            -- Create or replace the S3 stage
            CREATE OR REPLACE STAGE dev.raw_data.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
        snowflake_conn_id='snowflake_conn'
    )

    # Task 2: Load Data - Copy data from S3 into Snowflake tables
    load_data = SnowflakeOperator(
        task_id='load_data',
        sql="""
            -- Load data into user_session_channel table
            COPY INTO dev.raw_data.user_session_channel
            FROM @dev.raw_data.blob_stage/user_session_channel.csv;

            -- Load data into session_timestamp table
            COPY INTO dev.raw_data.session_timestamp
            FROM @dev.raw_data.blob_stage/session_timestamp.csv;
        """,
        snowflake_conn_id='snowflake_conn'
    )
    # Task dependencies
    set_stage >> load_data