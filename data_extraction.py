import pandas as pd
from sqlalchemy import text

class DataExtractor():   
    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        with engine.connect() as con:
            users_table = pd.DataFrame(con.execute(text(f'select * from {table_name}')))
        return users_table
