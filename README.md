# Multinational Retail Data Centralisation

This project aims to recreate the scenario of working for a MNC. A MNC with a diverse portfolio of goods and whose sales data is spread across multiple sources, thus, presenting the challenge of centralising said data and converting it into a more analysable, usable form.

## Project Motivation
- To gain practical insight into working with different data sources and the technologies involved in extracting that information for downstream teams.
- To reinforce OOP principles and demonstrate ability to effectively clean a dataset using Pandas-based transformations.

## Learning Points
- Understand use cases for .yaml files
- Learn how to use sqlalchemy, psycopg2, tabula and requests package to work with different data sources.
- How to work with pgadmin4 and set up a PostgreSQL database such that data can be imported into it and queried. 
- What the use cases are for Spark's Pandas API.
- Reminder of importance of taking advantage of Pandas vectorized operations as much as possible and the difference in various 'str' related functions.
- How to work with / query a PostgreSQL database remotely in VSC via extensions

## Project Breakdown

### Set up PostgreSQL database
- Created and configured a local PostgreSQL database using pgadmin4 in order to store the data to be extracted and cleaned in following steps.

![pgadmin4](https://github.com/mrmarq1/multinational-retail-data-centralisation/assets/126958930/cecdb780-599d-446f-b8b8-2cb50e071e86)

### User data

#### Extracted data
- Created a yaml file to store store database credientials required for data extraction.
- Utilised PyYAML package to return credentials as a dictionary.
- Used sqlalchemy engine with said credentials to initialise a databse engine.
- Created a method within a DataExtractor class to read data from a AWS RDS database.

#### Cleaned data 
- Created a method within the DataExtractor class to extract table relating to user data and convert it into a Pandas dataframe. 
- Created a method within the DataCleaning class to clean the user data and ensure correct data types.

#### Sent data to local database
- Created a method to connect to and send cleaned user data to local PostgreSQL database as a table named 'dim_users'.

### Credit card data

#### Extracted data
- Used tabula-py package to extract customer's card details from a PDF stored in a AWS S3 bucket.
- Created a method within the DataCleaning class to clean the user data and ensure correct datatypes.

#### Cleaned data 
- Created a method within the DataExtractor class to convert data into a Pandas dataframe. 
- Created a method within the DataCleaning class to clean the data and ensure correct data types.

#### Sent data to local database
- Called previously created method to send cleaned card data to local PostgreSQL database as a table named 'dim_card_details'.

### Store data

#### Extracted data
- Sourced data by using requests package to make GET requests to API based on an endpoint in which the number of stores first had to be discerned.

#### Cleaned data 
- Created a method within the DataExtractor class to convert data into a Pandas dataframe. 
- Created a method within the DataCleaning class to clean the data and ensure correct data types.

#### Sent data to local database
- Called previously created method to send cleaned store data to local PostgreSQL database as a table named 'dim_store_details'.

### Product data

#### Extracted data
- Created a method to extract csv data via s3 bucket endpoint and convert it to a Pandas DataFrame. 

#### Cleaned data 
- Created a method within the DataCleaning class to specifically handle the weights as required conversion and standardisation.
- Created another method to clean remainder of the dataset.

#### Sent data to local database
- Called previously created method to send cleaned product data to local PostgreSQL database as a table named 'dim_products'.

### Orders data

#### Extracted data
- Used previous method to extract data from AWS RDS database to get product order table.

#### Cleaned data 
- Created a method within the DataCleaning class to clean orders, including removing first_name, last_name and 1 columns so table columns match wuth other tables for subsequent star-based schema enforcement.

#### Sent data to local database
- Called previously created method to send cleaned orders data to local PostgreSQL database as a table named 'dim_orders'.

### Date events data

#### Extracted data
- Altered previous method for extracting csv data from a s3 bucket to take an argument for file type such that the date events data JSON file could be also extracted.

#### Cleaned data 
- Created a method within the DataCleaning class to clean date events and ensure correct data types.

#### Sent data to local database
- Called previously created method to send cleaned date events data to local PostgreSQL database as a table named 'dim_date_times'.

## Created database schema

- Used SQL to further amend tables in pgadmin4 via VSC extension, including casting the columns to correct data types.

![vsc_sql](https://github.com/mrmarq1/multinational-retail-data-centralisation/assets/126958930/c9c2e8ab-844b-4a77-b858-279350d9bf0c)

- Created primary and foreign keys to establish a star-based schema with the orders table at the centre.

![pgadmin4_orders](https://github.com/mrmarq1/multinational-retail-data-centralisation/assets/126958930/f38bca93-9df2-44f3-abba-ee0319880198)

## Queried data

- Queried the data with SQL, often leveraging JOIN statements based on star-based schema.
- Questions answered included: 'How many stores does the business have and in which country?', 'How many sales are coming from online?', 'What % of sales come via each store type?', 'Which month in each year produced the highest cost of sales?' and 'How quickly is the company making sales?'.
