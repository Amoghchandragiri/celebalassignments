from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import fastavro

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'export_data_dag',
    default_args=default_args,
    description='Export data from database to various formats',
    schedule_interval=timedelta(days=1),
)

def export_data():
    db_connection_str = 'mysql+pymysql://root:root@localhost/test_db'
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql('SELECT * FROM test_table', con=db_connection)
    df.to_csv('/path/to/output.csv', index=False)
    df.to_parquet('/path/to/output.parquet')
    
    def to_avro(df, file_name):
        records = df.to_dict(orient='records')
        schema = {
            "doc": "Data export",
            "name": "Data",
            "namespace": "namespace",
            "type": "record",
            "fields": [{"name": col, "type": ["null", "string"]} for col in df.columns],
        }
        with open(file_name, 'wb') as out:
            fastavro.writer(out, schema, records)
    
    to_avro(df, '/path/to/output.avro')

export_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data,
    dag=dag,
)

export_task
