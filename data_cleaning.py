import data_extraction, database_utils
import pandas as pd
import re

class DataCleaning:
    # AWS data
    def clean_user_data(self, table_name):
        connector_aws = database_utils.DatabaseConnector()
        
        extractor_aws = data_extraction.DataExtractor()
        df = extractor_aws.read_rds_table(connector_aws, table_name)

        df = df.sort_values('last_name').reset_index(drop=True)
        df = df.drop(df.index[:4]).reset_index(drop=True)
        user_id_duplicates = df[df['user_uuid'].duplicated(keep='first')==True]
        df = df.drop(user_id_duplicates.index)
        df_dob_filter = df[df['date_of_birth'].str.contains('.*-+|/+.*') == False]['date_of_birth']
        df_dob_filter = df_dob_filter[df_dob_filter.str.isupper()==True]
        df = df.drop(df_dob_filter.index).reset_index(drop=True)
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['join_date'] = pd.to_datetime(df['join_date'])
        df['country_code'] = df['country_code'].apply(lambda x : x.replace('GGB', 'GB'))
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
       
# Clean AWS data and export
cleaner_aws = DataCleaning()
cleaner_aws.clean_user_data('legacy_users')
