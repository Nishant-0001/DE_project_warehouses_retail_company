import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
aws_access_key = "eonh06rTdM+xDSJnOOhS1ZxPzrONTYyLXx4tNImZIYg="
aws_secret_key = "cTwRDIIV3l59tvIZ988TEUHX6MW9iTGJIAiccexWQgFBeSINESpGsJHDpX8YtOvl"
bucket_name = "data-project-testing01"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
host="localhost"
database_name = "data_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "D:\\DE Project\\file_from_s3\\"
customer_data_mart_local_file = "D:\\DE Project\\customer_data_mart\\"
sales_team_data_mart_local_file = "D:\\DE Project\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "D:\\DE Project\\partition_data\\"
error_folder_path_local = "D:\\DE Project\\error_files\\"
