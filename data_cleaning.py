import data_extraction, database_utils, header_key
import numpy as np
import pandas as pd
import re
from pandas.tseries.offsets import MonthEnd

class DataCleaning:
    # AWS user data
    def clean_user_data(self):
        connector_aws = database_utils.DatabaseConnector()
        extractor_aws = data_extraction.DataExtractor()
        df = extractor_aws.read_rds_table(connector_aws, 'legacy_users')

        df= df.rename(columns={'index':'idx'})
        user_id_duplicates = df[df['user_uuid'].duplicated(keep='first')==True]
        df = df.drop(user_id_duplicates.index)
        df_dob_filter = df[df['date_of_birth'].str.contains('.*-+|/+.*') == False]['date_of_birth']
        df_dob_filter = df_dob_filter[df_dob_filter.str.isupper()==True]
        df = df.drop(df_dob_filter.index)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['join_date'] = pd.to_datetime(df['join_date'])
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')
        df[['country_code','country']] = df[['country_code','country']].astype('category')
        df['phone_number'] = df['phone_number'].str.replace('[()\-x\s.,]', '', regex=True)
        def add_country_code(row):
          mapping = {'GB': '44', 'DE': '49', 'US': '01'}
          number = row['phone_number']
          if re.match('[^+]', number):
            row['phone_number'] = mapping[row['country_code']] + number
            return row
          else:
            row['phone_number'] = re.sub('\+','',number)
            return row           
        df = df.apply(add_country_code, axis=1)
        df['address'] = df['address'].str.replace(r'\n',' ',regex=True)
        to_string_cols = df.iloc[:, np.r_[1:3,4:10,11]]
        df[to_string_cols.columns] = to_string_cols.astype('string')
        df = df.reset_index(drop=True)

        connector_aws.upload_to_db(df, 'dim_users')

    # PDF card data 
    def clean_card_data(self):
        extractor_pdf = data_extraction.DataExtractor()
        dfs = extractor_pdf.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        df = pd.concat([df for df in dfs])
        df = df.reset_index(drop=True)
        
        str_card_numbers = df[df['card_number'].apply(type) == str]
        non_numerical_card_numbers = str_card_numbers['card_number'].str.findall('[^0-9]')
        df = df.drop(non_numerical_card_numbers.index).reset_index(drop=True)
        df['expiry_date'] = df['expiry_date'].str.replace('/','20')
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m%Y') + MonthEnd(0)
        df['card_provider'] = df['card_provider'].astype('category')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'])
        df['card_number'] = df['card_number'].astype('string')

        connector_pdf = database_utils.DatabaseConnector()
        connector_pdf.upload_to_db(df, 'dim_card_details')
    
    # API store data
    def clean_store_data(self):
        extractor_api = data_extraction.DataExtractor()
        extractor_api.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header_key.KEY)
        df = extractor_api.retrieve_store_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store}', header_key.KEY)
        
        df = df.rename(columns={'index':'idx'})
        df['address'] = df['address'].str.replace('[-,/\.(\n) ]',' ', regex=True)
        filter = df[df['address'].str.contains('\s')==False]
        df = df.drop(filter.index)
        filter = df[df['longitude'].str.contains('[\d.]')==False]
        df = df.drop(filter.index)
        filter = df[df['staff_numbers'].str.isnumeric()==False]
        df = df.drop(filter.index)
        df['staff_numbers'] = df['staff_numbers'].astype('int')
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        df[df['latitude'].str.contains('[\d.]')==False]
        df[['latitude', 'longitude', 'lat']] = df[['latitude','longitude','lat']].astype('float')
        df[df['continent'] == ('eeAmerica' and 'eeEurope')]
        df['continent'] = df['continent'].str.replace('eeAmerica','America').str.replace('eeEurope','Europe')
        df[['continent', 'country_code', 'store_type','locality']] = df[['continent', 'country_code', 'store_type','locality']].astype('category')
        df[['address','store_code']] = df[['address','store_code']].astype('string')
        df = df.drop('lat', axis=1)
        df = df.reset_index(drop=True)

        connector_api = database_utils.DatabaseConnector()
        connector_api.upload_to_db(df, 'dim_store_details')

    # S3 products data - weight conversion
    def convert_product_weights(self, df):        
        irregular_weights = df[df['weight'].str.contains('[A-Z]')==True]
        df = df.drop(irregular_weights.index)

        non_kg_filter = df[df['weight'].str.contains('kg') == False]
        oz_filter = non_kg_filter[non_kg_filter['weight'].str.contains('oz')==True]
        oz_filter['weight'] = (oz_filter['weight'].str.replace('oz','').astype(float)) * 28.3495
        non_kg_filter.loc[oz_filter.index, 'weight'] = oz_filter['weight']

        non_kg_filter['weight'] = non_kg_filter['weight'].str.replace('ml','').str.replace('g','').str.replace(' ','').str.replace('x','*')
        weigths_to_calc = non_kg_filter[non_kg_filter['weight'].str.contains('\*') == True]
        calc_weights = pd.Series(pd.eval(weigths_to_calc['weight']), weigths_to_calc.index)
        non_kg_filter.loc[calc_weights.index, 'weight'] = calc_weights
        non_kg_filter['weight'] = non_kg_filter['weight'].astype(float)
        non_kg_filter['weight'] = non_kg_filter['weight'] / 1000
        df.loc[non_kg_filter.index,'weight'] = non_kg_filter['weight']
        df['weight'] = df['weight'].astype('string')
        df['weight'] = df['weight'].str.replace('kg','', regex=False).astype('float')

        return df
    
    # S3 products data
    def clean_products_data(self):
        extractor_s3 = data_extraction.DataExtractor()
        df = extractor_s3.extract_from_s3('s3://data-handling-public/products.csv', 'csv')
        df = self.convert_product_weights(df)
        
        nan_rows = df[df.isna().any(axis=1)]
        df = df.drop(nan_rows.index)
        df['product_price'] = df['product_price'].str.replace('??','')
        df['product_price'] = df['product_price'].astype('float')
        df['date_added'] = pd.to_datetime(df['date_added'])
        df[['category','removed']] = df[['category','removed']].astype('category')
        df[['product_name','EAN','uuid','product_code']] = df[['product_name','EAN','uuid','product_code']].astype('string')
        df = df.drop('Unnamed: 0', axis=1)
        df = df.reset_index(drop=True)

        connector_s3 = database_utils.DatabaseConnector()
        connector_s3.upload_to_db(df, 'dim_products')

    # AWS orders data
    def clean_orders_data(self):
        connector_aws = database_utils.DatabaseConnector()
        extractor_aws = data_extraction.DataExtractor()
        df = extractor_aws.read_rds_table(connector_aws, 'orders_table')
        
        df = df.rename(columns={'index':'idx'})
        df = df.drop(['first_name','last_name','1'], axis=1)
        df['product_quantity'] = df['product_quantity'].astype('category')
        to_string_cols = ['date_uuid','user_uuid','store_code','product_code']
        df[to_string_cols] = df[to_string_cols].astype('string')
        df = df.reset_index(drop=True)

        connector_aws.upload_to_db(df, 'orders_table')

    def clean_date_times_data(self):
        extractor_s3 = data_extraction.DataExtractor()
        df = extractor_s3.extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json', 'json')
        
        filter = df[df['timestamp'].str.contains('[A-Za-z]')==True]
        df = df.drop(filter.index)
        df['datetime'] = df['day']+'/'+df['month']+'/'+df['year']+' '+df['timestamp']
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.drop(['timestamp','month','year','day'], axis=1)
        df['time_period'] = df['time_period'].astype('category')
        df['date_uuid'] = df['date_uuid'].astype('string')
        df = df.reset_index(drop=True)

        connector_s3 = database_utils.DatabaseConnector()
        connector_s3.upload_to_db(df, 'dim_date_times')

cleaner = DataCleaning()

#cleaner.clean_user_data()
#cleaner.clean_card_data()
#cleaner.clean_store_data()
#cleaner.clean_products_data()
#cleaner.clean_orders_data()
cleaner.clean_date_times_data()