from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import requests
import logging

# Retrieve the Alpha Vantage API key from Airflow Variables
ALPHA_VANTAGE_API_KEY = Variable.get('apikey')

with DAG(
    dag_id='amzn_stock_data_pipeline',
    default_args={'owner': 'deva', 'start_date': days_ago(1)},
    schedule_interval='@daily',
    catchup=False,
    tags=['ETL', 'AlphaVantage', 'Snowflake']
) as dag:

    @task
    def retrieve_stock_data(stock_symbol='AMZN'):
        """Fetch daily stock data from Alpha Vantage API."""
        try:
            api_url = (
                f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}'
                f'&apikey={ALPHA_VANTAGE_API_KEY}'
            )
            response = requests.get(api_url)
            response.raise_for_status()
            stock_data_json = response.json()

            stock_records = []
            for date, data in stock_data_json["Time Series (Daily)"].items():
                data["date"] = date
                data["symbol"] = stock_symbol
                stock_records.append(data)

            logging.info(f"Successfully retrieved stock data for {stock_symbol}")
            return stock_records

        except Exception as e:
            logging.error(f"Failed to fetch stock data for {stock_symbol}: {str(e)}")
            raise

    initialize_snowflake_resources = SnowflakeOperator(
        task_id='initialize_snowflake_resources',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Initialize warehouse
        CREATE WAREHOUSE IF NOT EXISTS SNOWFLAKE_WAREHOUSE
        WITH WAREHOUSE_SIZE = 'SMALL'
        AUTO_SUSPEND = 300
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE;

        -- Initialize database and schema
        CREATE DATABASE IF NOT EXISTS SNOWFLAKE_DATABASE;
        CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_DATABASE.api_data;

        -- Set context to the newly created database and schema
        USE DATABASE SNOWFLAKE_DATABASE;
        USE SCHEMA api_data;

        -- Create the table if it doesn't exist
        CREATE TABLE IF NOT EXISTS stock_prices (
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume INT,
            date VARCHAR(512) PRIMARY KEY,
            symbol VARCHAR(20)
        );
        """,
        warehouse='SNOWFLAKE_WAREHOUSE'
    )

    @task
    def prepare_insert_query(stock_records):
        """Prepare SQL insert statements for the fetched stock data."""
        insert_values = []
        for record in stock_records:
            open_price = float(record['1. open'])
            high_price = float(record['2. high'])
            low_price = float(record['3. low'])
            close_price = float(record['4. close'])
            volume = int(record['5. volume'])
            date = record['date']
            symbol = record['symbol']

            insert_values.append(
                f"({open_price}, {high_price}, {low_price}, {close_price}, {volume}, '{date}', '{symbol}')"
            )

        insert_query = f"""
        USE DATABASE SNOWFLAKE_DATABASE;
        USE SCHEMA api_data;
        INSERT INTO stock_prices (open, high, low, close, volume, date, symbol)
        VALUES {', '.join(insert_values)};
        """
        return insert_query

    load_data_into_snowflake = SnowflakeOperator(
        task_id='load_data_into_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql=prepare_insert_query(retrieve_stock_data('AMZN')),
        warehouse='SNOWFLAKE_WAREHOUSE'
    )

    validate_snowflake_data = SnowflakeOperator(
        task_id='validate_data_in_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql="""
        USE DATABASE SNOWFLAKE_DATABASE;
        USE SCHEMA api_data;
        SELECT COUNT(*), AVG(close) FROM stock_prices;
        """,
        warehouse='SNOWFLAKE_WAREHOUSE'
    )

    # Define task dependencies
    initialize_snowflake_resources >> load_data_into_snowflake >> validate_snowflake_data
