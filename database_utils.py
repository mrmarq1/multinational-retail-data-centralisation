import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as stream:
            data = yaml.safe_load(stream)
            return data
    
    def read_local_db_creds(self):
        with open('sales_data_db.yaml', 'r') as stream:
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

    def upload_to_db(self, df, table_name):
        data = self.read_local_db_creds()
        engine = create_engine('postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}'.format(**data))
        df.to_sql(name=table_name, con=engine, if_exists='replace')