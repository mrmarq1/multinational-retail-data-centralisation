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
        self.num_stores = num_stores_json['number_stores']
        return self.num_stores
    
    def retrieve_store_data(self, stores_endpoint, header):
        stores_df = pd.DataFrame()

        for store in range(self.num_stores):
            endpoint = stores_endpoint.format(store=store)
            response = requests.get(endpoint, headers=header)
            store_df = pd.DataFrame(response.json(), index=[store])
            stores_df = pd.concat([stores_df,store_df])
        
        return stores_df

    def extract_from_s3(self, link, file_type):
        if file_type=='csv':
            df = pd.read_csv(link)
        if file_type=='json':
            df = pd.read_json(link)    
        return df


