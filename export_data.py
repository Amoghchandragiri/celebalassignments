import pandas as pd
from sqlalchemy import create_engine
import fastavro

# Database connection setup
db_connection_str = 'mysql+pymysql://root:root@localhost/test_db'
db_connection = create_engine(db_connection_str)

# Load data from a table
df = pd.read_sql('SELECT * FROM test_table', con=db_connection)

# Export to CSV
df.to_csv('output.csv', index=False)

# Export to Parquet
df.to_parquet('output.parquet')

# Export to Avro
def to_avro(df, file_name):
    records = df.to_dict(orient='records')
    schema = {
        "doc": "Data export",
        "name": "Data",
        "namespace": "namespace",
        "type": "record",
        "fields": [
            {"name": "id", "type": ["null", "int"]},  # Allow null and int for 'id'
            {"name": "name", "type": ["null", "string"]},  # Allow null and string for 'name'
            {"name": "value", "type": ["null", "int"]}  # Allow null and int for 'value'
        ],
    }
    with open(file_name, 'wb') as out:
        fastavro.writer(out, schema, records)

to_avro(df, 'output.avro')
