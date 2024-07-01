from sqlalchemy import create_engine
import pandas as pd

source_db_connection_str = 'mysql+pymysql://root:root@localhost/source_db'
target_db_connection_str = 'mysql+pymysql://root:root@localhost/target_db'

source_engine = create_engine(source_db_connection_str)
target_engine = create_engine(target_db_connection_str)

# Specify your tables and columns
tables_columns = {
    'table1': ['column1', 'column2'],
    'table2': ['column3', 'column4'],
}

for table, columns in tables_columns.items():
    query = f'SELECT {", ".join(columns)} FROM {table}'
    df = pd.read_sql_query(query, source_engine)
    df.to_sql(table, target_engine, if_exists='replace', index=False)
