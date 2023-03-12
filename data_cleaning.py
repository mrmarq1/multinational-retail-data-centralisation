import data_extraction, database_utils
import pandas as pd
import re
from pandas.tseries.offsets import MonthEnd

class DataCleaning:
    # AWS user data
    def clean_user_data(self):
        connector_aws = database_utils.DatabaseConnector()
        
        extractor_aws = data_extraction.DataExtractor()
        df = extractor_aws.read_rds_table(connector_aws, 'legacy_users')

        df = df.sort_values('last_name').reset_index(drop=True)
        df = df.drop(df.index[:4]).reset_index(drop=True)
        user_id_duplicates = df[df['user_uuid'].duplicated(keep='first')==True]
        df = df.drop(user_id_duplicates.index)
        df_dob_filter = df[df['date_of_birth'].str.contains('.*-+|/+.*') == False]['date_of_birth']
        df_dob_filter = df_dob_filter[df_dob_filter.str.isupper()==True]
        df = df.drop(df_dob_filter.index).reset_index(drop=True)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['join_date'] = pd.to_datetime(df['join_date'])
        df['country_code'] = df['country_code'].str.replace('GGB', 'GB')
        df['country_code'] = df['country_code'].astype('category')
        df['country'] = df['country'].astype('category')
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
        non_numerical_expiries = df[df['expiry_date'].apply(lambda full_num: any(num.isalpha() for num in full_num)) == True]
        df = df.drop(non_numerical_expiries.index).reset_index(drop=True)
        df['expiry_date'] = df['expiry_date'].str.replace('/','20')
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m%Y') + MonthEnd(0)
        df['card_provider'] = df['card_provider'].astype('category')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'])
        
        connector_pdf = database_utils.DatabaseConnector()
        connector_pdf.upload_to_db(df, 'dim_card_details')
    
    # API data
    def clean_store_data(self):
        extractor_api = data_extraction.DataExtractor()
        extractor_api.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', header_key.KEY)
        df = extractor_api.retrieve_store_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store}', header_key.KEY)

        df['address'] = df['address'].str.replace('[-,/\.(\n) ]',' ', regex=True)
        filter = df[df['address'].str.contains('\s')==False]
        df = df.drop(filter.index)
        filter = df[df['longitude'].str.contains('[\d.]')==False]
        df = df.drop(filter.index)
        df['longitude'] = df['longitude'].astype('float')
        df['lat'] = df['lat'].astype('float')
        df['locality'] = df['locality'].astype('category')
        df['store_code'].str.findall('[^A-Z0-9-]').value_counts()
        filter = df[df['staff_numbers'].str.isnumeric()==False]
        df = df.drop(filter.index)
        df['staff_numbers'] = df['staff_numbers'].astype('int')
        df['opening_date'] = pd.to_datetime(df['opening_date'])
        df['store_type'] = df['store_type'].astype('category')
        df[df['latitude'].str.contains('[\d.]')==False]
        df['latitude'] = df['latitude'].astype('float')
        df['country_code'] = df['country_code'].astype('category')
        df[df['continent'] == ('eeAmerica' and 'eeEurope')]
        df['continent'] = df['continent'].str.replace('eeAmerica','America').str.replace('eeEurope','Europe')
        df['continent'] = df['continent'].astype('category')
        df = df.drop('lat', axis=1)
        df = df.reset_index(drop=True)
        
        return df


cleaner = DataCleaning()

#cleaner.clean_user_data()
#cleaner.clean_card_data()
cleaner.clean_store_data()
