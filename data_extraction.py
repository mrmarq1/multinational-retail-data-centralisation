import header_key

import pandas as pd
from sqlalchemy import text
import tabula
import requests
 
class DataExtractor():   

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        with engine.connect() as con:
           users_table = pd.DataFrame(con.execute(text(f'select * from {table_name}')))
        return users_table

    def retrieve_pdf_data(self, pdf_path):
        df = tabula.read_pdf(pdf_path, pages='all', stream=True)
        return df

    def list_number_of_stores(self, num_stores_endpoint, header):
        response = requests.get(num_stores_endpoint, headers=header)
        num_stores_json = response.json()
        num_stores = num_stores_json['number_stores']
        return num_stores

extractor = DataExtractor()
num_stores = extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header_key.KEY)
print(num_stores)
