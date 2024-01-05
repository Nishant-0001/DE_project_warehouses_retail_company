import datetime
import os
import shutil
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from resources.dev.config import *
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.read.database_read import *
from src.main.utility.spark_session import spark_session
from src.main.transformations.jobs import dimension_tables_join
from src.main.transformations.jobs.dimension_tables_join import *
from src.main.transformations.jobs.customer_mart_sql_tranform_write import *
from src.main.write.parquet_writer import *
from src.main.write.sales_mart_calculation_table_write import sales_mart_calculation_table_write

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

######################get s3 client
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

##############################you can use s3 client for operations
response = s3_client.list_buckets()
logger.info("list of buckets %s", response['Buckets'])

# check if local directory has already a file
# if file is there check if same file is present in staging area
# with status as A if not try to re-run it
# else give error and process next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"""
                 select distinct file_name from
                 {config.database_name}.{config.product_staging_table}
                 where file_name in({str(total_csv_files)[1:-1]}) and status ="A"
    
     """

    logger.info(f"dynamically created statement :{statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("last run was unsuccessful")
    else:
        logger.info("no recorded found")
else:

    logger.info("last run was success")

try:
    s3_reader = S3Reader()
    # try bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
    logger.info("absolute path on s3 bucket for csv file %s", s3_absolute_file_path)

    if not s3_absolute_file_path:
        logger.info(f"no files found in :{folder_path}")
        raise Exception("no data available to process")
except Exception as e:
    logger.error("exited with error: %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory
prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info(f"like path is available on s3 bucket : %s and folder name is %s", bucket_name, file_paths)
logging.info(f"file path available on s3 under {bucket_name} bucket and folder is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("file download error :%s",e)
    sys.exit()
# get list of all  files in local directory

all_files = os.listdir(local_directory)
logger.info(f"list of all files present on directory after download {all_files}")

# filter files .csv I there names and create absolute path

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))
    if not csv_files:
        logger.error("no csv data available to process request")
        raise Exception("no csv data available to process request")
else:
    logger.error("there is no data to process")
    raise Exception("there is no data to process")

logger.info("******************************list of files********************")
logger.info("*****************list of files needs to be processed %s ***************", csv_files)
logger.info("****************************starting spark*******************")
spark = spark_session()
logger.info("*********************spark session created **************************")


#cherck required column in schema of files
#if not required columns are found keep it in error _files
#else union all data in one data frame

logger.info("*********checking schema*************")

correct_files=[]
for data in csv_files:
    data_schema=spark.read.format("csv")\
        .option("header","true")\
        .load(data).columns
    logger.info(f"schema for {data} is {data_schema}")
    logger.info(f"mandatory columns for schema{config.mandatory_columns}")
    missing_columns=set(data_schema)-set(config.mandatory_columns)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"no missing columns for the {data}")
        correct_files.append(data)


logger.info(f"**********list of corrected file{correct_files}**********")
logger.info(f"*****list of error file {error_files}***********")
logger.info(f"****moving error data to error directory if any*********")

error_folder_local_path=config.error_folder_path_local
if error_files:
    for file_paths in error_files:
        if os.path.exists(file_paths):
            file_name=os.path.basename(file_paths)
            destination_path=os.path.join(error_folder_local_path,file_name)

            shutil.move(file_paths,destination_path)
            logger.info(f"moved {file_name} file from s3 bucket to {destination_path}")

            source_prefix=config.s3_source_directory
            destination_prefix=config.s3_error_directory

            message=move_s3_to_s3(s3_client,bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"{file_paths} does not exist")
    else:
        logger.info("********there is no error file available in our data set")

#adtionall column needs to be taken care of
#determining extra column
#staging table needs to updated "A" as active and "I" is inactive

logger.info("*************updating staging table**************")
insert_statement=[]
db_name=config.database_name

current_date = datetime.datetime.now()
formatted_date=current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name=os.path.basename(file)
        statement=f""" insert into {db_name}.{config.product_staging_table}
                        (file_name,file_location,created_date,status)
                        values('{file_name}','{file_name}','{formatted_date}','A')
                    """
        insert_statement.append(statement)
    logger.info(f"insert into statement created :{insert_statement}")
    logger.info(f"**********connecting to my sql server***************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info("********sql connection created successfully*********")
    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("no correct files to process")
    raise Exception("******no data available with correct file to process******")
logger.info("****************staging table updated successfully *********")
logger.info("*****************fixing extra column coming from source *******************")



#connecting with database

database_client=DatabaseReader(config.url,config.properties)
logger.info("********creating empty dataframe*****")
final_df_to_process=database_client.create_dataframe(spark, "empty_df_create_table")

#final_df_to_process = spark.createDataFrame([],schema=schema)


for data in correct_files:
    data_df=spark.read.format("csv")\
        .option("header","true")\
        .option("InferSchema","true")\
        .load(data)
    data_schema=data_df.columns
    extra_columns=list(set(data_schema)-set(config.mandatory_columns))
    logger.info(f"Extra columns present at the source is{extra_columns}")
    if extra_columns:
        data_df=data_df.withColumn("additional column",concat_ws(",",*extra_columns))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                    "price","quantity","additional_column","total_cost")
    else:
        data_df=data_df.withColumn("additional_column",lit(None))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                    "price","quantity","additional_column","total_cost")

    final_df_to_process=final_df_to_process.union(data_df)

logger.info("*****************final data frame which is  going to process")



# enrich data from all dimension table
#create a data mart for sales team and their incentives,address and all
#another data mart for customer who bought how much  each day of month
#for every month there should be file inside that
#there should be store id segregation
#read data from parquet and generate csv
#in which there should be a sale person name and ID
#salse_person total billing for each month ,total

database_client=DatabaseReader(config.url,config.properties)
#creating df for all tables
#customer table
logger.info("*********CREATING customer table************")
customer_table_df=database_client.create_dataframe(spark,config.customer_table_name)
#product_table
logger.info("************loading staging table to product staging table************")
product_staging_table_df=database_client.create_dataframe(spark,config.product_staging_table)

#sales_team_table
logger.info("******loading sales team table into sales team table***********")
sales_team_table_df=database_client.create_dataframe(spark,config.sales_team_table)

#store table
logger.info("**********loading store table into store data frame")
store_table_df=database_client.create_dataframe(spark,config.store_table)

#joining table with
s3_customer_store_sales_df_join=dimesions_table_join(final_df_to_process,customer_table_df,
                                                      store_table_df,sales_team_table_df, )

############final enriched data
logger.info("final enriched data")
s3_customer_store_sales_df_join.show()


#write customer data into customer data mart in parquet format
#file wil be written to local first
#move  raw data to s3 bucket for reporting tool
#write reporting data in mysql table also

logger.info("*********writing data in to customer data mart ******")
final_customer_data_mart_df=s3_customer_store_sales_df_join\
    .select("ct.customer_id","ct.first_name",
            "ct.last_name","ct.address","ct.pincode","phone_number","sales_date","total_cost")
logger.info("**********final data for customer data mart************ ")
#final_customer_data_mart_df.show()



parquet_writer=ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,
                                config.customer_data_mart_local_file)

logger.info(f"customer data written to local disk at {config.customer_data_mart_local_file}")


s3_uploader=UploadToS3(s3_client)
s3_directory=config.s3_customer_datamart_directory
message=s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#salse_team_data_mart

logger.info("write data into sales data mart *********")
final_sales_team_data_mart_df=s3_customer_store_sales_df_join\
    .select("store_id","sales_person_id","sales_person_first_name","sales_person_last_name",
            "store_manager_name","manager_id","is_manager","sales_person_address","sales_person_pincode",
            "sales_date","total_cost",
            expr("SUBSTRING(sales_date,1,7) as sales_month"))
logger.info("**********final data for sales data mart************ ")
final_sales_team_data_mart_df.show()
parquet_writer=ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,
                                config.sales_team_data_mart_local_file)

logger.info(f"*****sales team data mart written at local disk at {config.sales_team_data_mart_local_file}")


#move_data_on_s3_bucket_fo_sales_data_mart

s3_uploader=UploadToS3(s3_client)
s3_directory=config.s3_sales_datamart_directory
message=s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#writing data partition
final_sales_team_data_mart_df.write.format("parquet")\
    .mode("append") \
    .option("header","true") \
    .partitionBy("sales_month","store_id")\
    .option("path",config.sales_team_data_mart_partitioned_local_file)\
    .save()
#move data on s3 to partitioned folder

s3_prefix="sales_partitioned_data_mart"
current_epoch=int(datetime.datetime.now().timestamp()) *1000
for root,dirs,files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        #print(file)
        local_file_path=os.path.join(root,file)
        relative_file_path=os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key=f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)


 # calculation for customer mart
logger.info("calculating customer's every month purchased amount ")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("calculation of customer mart done and written in table")


logger.info("calculating sales's every month purchased amount ")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("calculation of sales mart done and written in table")


#  move s3 data to processed folder and deleting from local files

source_prefix=config.s3_source_directory

destination_prefix=config.s3_processed_directory
message=move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix)
logger.info(f"{message}")

logger.info("deleing sales_data from local ")
delete_local_file(config.local_directory)
logger.info("deleted sales data from local")

logger.info("deleting customer data mart from local")
delete_local_file(config.customer_data_mart_local_file)
logger.info("deleted customer data mart from local")

logger.info("deleting sales team data mart from local")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("deleted salse team data mart from local")

logger.info("deleting sales team data mart partioned file from local")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("deleted sales team data mart partioned file from local")


#updating status of satging table
update_statements=[]
if correct_files:
    for file in correct_files:
        file_name=os.path.basename(file)
        statements = f"""UPDATE {db_name}.{config.product_staging_table}
                        SET STATUS = 'I', updated_date = '{formatted_date}'
                       WHERE file_name = '{file_name}'
"""
    update_statements.append(statements)
    logger.info(f"update  statement created for staging table:{update_statements}")
    logger.info(f"**********connecting to my sql server***************")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info("********sql connection created successfully*********")
    for statement in update_statements:
        cursor.execute(statements)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("***************there is some error in process in between***********")
    sys.exit()

input("press enter to terminate")
