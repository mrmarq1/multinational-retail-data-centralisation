import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as stream:
            data = yaml.safe_load(stream)
            return data

    def init_db_engine(self):
        data = self.read_db_creds()
        engine = create_engine("postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}".format(**data))           
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        print(inspector.get_table_names())

# get_tables = DatabaseConnector()
# get_tables.list_db_tables()
