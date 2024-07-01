from sqlalchemy import create_engine, inspect
import pandas as pd

source_db_connection_str = 'mysql+pymysql://root:root@localhost/source_db'
target_db_connection_str = 'mysql+pymysql://root:root@localhost/target_db'

source_engine = create_engine(source_db_connection_str)
target_engine = create_engine(target_db_connection_str)

inspector = inspect(source_engine)
tables = inspector.get_table_names()

for table in tables:
    df = pd.read_sql_table(table, source_engine)
    df.to_sql(table, target_engine, if_exists='replace', index=False)
