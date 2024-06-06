#finalfinal
import datetime
import os.path
import shutil

from pyspark.sql.functions import *
from pyspark.sql.types import *

from resources.dev import config
from resources.dev.config import customer_table_name
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import  *
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

#get s3 client
aws_access_key = config.aws_access_key

#aws access key = "CockQBSLLHHDrx5r6HY4idWg/Ofkw7wRmBFG8MUfbRU="
aws_secret_key= config.aws_secret_key

s3_client_provider=S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

#

response=s3_client.list_buckets()
print(response)
logger.info("List of Buckets: %s",response['Buckets'])




csv_files = [file for file in os.listdir(config.local_directory)if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"""
    select distinct file_name from {config.database_name}.product_staging_table where file_name in ({str(total_csv_files)[1:-1]}) and status='A'
    """

    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data=cursor.fetchall()
    if data:
        logger.info("your last run was failed")
    else:
        logger.info("no record match")
else:
    logger.info("last run was successful!!!!!!!!!!!!!")



try:
    s3_reader = S3Reader()
    folder_path= config.s3_source_directory
    s3_absolute_file_path= s3_reader.list_files(s3_client,config.bucket_name,folder_path=folder_path)
    logger.info("absolute path on s3 bucket for csv file %s",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"no files available at {folder_path}")
        raise Exception("no data available to process")
except Exception as e:
    logger.error("exited with error:%s",e)
    raise e




bucket_name=config.bucket_name
local_directory=config.local_directory

prefix = f"s3://{bucket_name}/"
file_path = [url[len(prefix):]for url in s3_absolute_file_path]
logging.info("file path available on s3 under %s bucket and folder name is %s",bucket_name,file_path)
logging.info(f"file path available on path s3 under {bucket_name} bucket and folder name is {file_path}")
try:
    downloader = S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_path)
except Exception as e:
    logger.error("file download error: %s",e)
    sys.exit()


all_files = os.listdir(local_directory)
logger.info(f"total files available in local directory after download {all_files}")

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))

    if not csv_files:
        logger.error("no csv data available to process")
        raise Exception("no csv data available to process")

else:
    logger.error("theres no data to process")
    raise Exception("theres no data to process")



logger.info("*********listing the file**********")
logger.info(f"listing the csv files %s {csv_files}")

logger.info("*********creating spark session**********")
spark = spark_session()
logger.info("**********spark session created**********")



logger.info("*********** checking schema for data loaded in s3 *********")
correct_files=[]
for data in csv_files:
    data_schema=spark.read.format("csv")\
        .option("header","true")\
        .load(data).columns
    logger.info(f"schema of the {data} is {data_schema}")
    logger.info(f"mandatory column schema is {config.mandatory_columns}")
    missing_columns= set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"theres no missing columns for {data}")
        correct_files.append(data)

logger.info(f"*******list of correct files*******{correct_files}")
logger.info(f"*******list of error files*******{error_files}")
logger.info("******* moving error data to error directory if any *******")



#moving data to error directory
error_folder_local_path = config.error_folder_path_local

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name=os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"moved {file_name} s3 file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(message)
        else:
            logger.info(f"{file_path} does not exist")
else:
    logger.info("************ no error files available ************** ")





#updating staging table

logger.info("******* updating the product staging table that we started a process *******")
insert_statements = []
db_name=config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f""" insert into {db_name}.{config.product_staging_table}
                     (file_name,file_location,created_date,status)
                     VALUES ('{filename}','{filename}','{formatted_date}', 'A')"""
        insert_statements.append(statements)
    logger.info(f"insert statement created for staging table --- {insert_statements}")
    logger.info("******* connecting with mysql server *******")
    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info("******* mysql server connected successfully *******")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("******* theres no files to process *******")
    raise Exception("******* no data available with correct files *******")


logger.info("******** staging table updated successfully **********")

logger.info("******* fixing extra column coming from source *******")


schema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("store_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("sales_date",DateType(),True),
    StructField("sales_person_id",IntegerType(),True),
    StructField("price",FloatType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("total_cost",FloatType(),True),
    StructField("additional_column",StringType(),True)
])

logger.info("********* creating empty dataframe *********")


final_df_to_process=spark.createDataFrame([],schema=schema)
final_df_to_process.show()
for data in correct_files:
    data_df=spark.read.format("csv")\
        .option("header","true")\
        .option("inferSchema","true")\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"extra columns present at source are {extra_columns}")
    if extra_columns:
        data_df=data_df.withColumn("additional_column",concat_ws(",",*extra_columns))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")
        logger.info(f"processed the {data} and added 'additional_column'")
    else:
        data_df=data_df.withColumn("additional_column",lit(None))\
            .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info("******** Final dataframe ready to process ***********")
final_df_to_process.show()



#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////










#database_client=DatabaseReader(config.url,config.properties)

#logger.info("********** loding customer table into customer_table_df ***********")
#customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)






#database_client = DatabaseReader(config.url, config.properties)
#customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
#customer_table_df.show()
#
#customer_table_df = spark.read.format('jdbc')\
#                .option('driver','com.mysql.cj.jdbc.Driver')\
#                .option('user','root')\
#                .option('password','arbaz')\
#                .option('dbtable','youtube_project.customer')\
#                .load()

#customer_table_df.show()


#df = spark.read.jdbc(url=config.url, table=config.customer_table_name, properties=config.properties)









#from pyspark.sql import SparkSession
#
#spark = spark_session()

#jdbc_url = "jdbc:mysql://your-mysql-host:3306/your-database"
#connection_properties = {
#    "user": "root",
#    "password": "arbaz",
#    "driver": "com.mysql.cj.jdbc.Driver"
#}


#df = spark.read.jdbc(url=jdbc_url, table=config.customer_table_name, properties=connection_properties)
#df.show()









#\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\

logger.info("******* loading customer table into customer_table_df ********")
customer_table_df= spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("C:\\Users\\arbaz\\Desktop\\jptr\\from_sql_customer.csv")
#customer_table_df.show()




logger.info("******* loading product table into product_table_df ********")


product_table_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("C:\\Users\\arbaz\\Desktop\\jptr\\from_sql_product.csv")
#product_table_df.show()



logger.info("************* loading staging table into product_staging_table_df ************")

product_staging_table_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("C:\\Users\\arbaz\\Desktop\\jptr\\from_sql_product_staging_table.csv")
#product_staging_table_df.show()



logger.info("********** loading sales team table into sales_team_table_df ********* ")

sales_team_table_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","True")\
    .load("C:\\Users\\arbaz\\Desktop\\jptr\\from_sql_sales_team.csv")
#sales_team_table_df.show()



logger.info("******** loading store table into store_table_df ***********")
store_table_df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("C:\\Users\\arbaz\\Desktop\\jptr\\from_sql_store.csv")
#store_table_df.show()



s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)

logger.info("******************************* FINAL ENRICHED DATA******************************")
s3_customer_store_sales_df_join.show()





# customer data mart
logger.info("************* write the data into customer mart ***********")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                .select("ct.customer_id",
                        "ct.first_name",
                        "ct.last_name",
                        "ct.address",
                        "ct.pincode",
                        "ct.phone_number",
                        "sales_date",
                        "total_cost")

logger.error("************ final data for customer data mart *******************")
final_customer_data_mart_df.show()



parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,
                                config.customer_data_mart_local_file)

logger.info(f"**********customer data written to local disk at {config.customer_data_mart_local_file}************")



#move data on s3 bucket (customer data mart)
logger.info("********** moving data from local to s3 for customer data mart **************")
s3_uploader = UploadToS3(s3_client)
s3_directory1 = config.s3_customer_datamart_directory
message1 = s3_uploader.upload_to_s3(s3_directory1,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message1}")

#sales team data mart
logger.info("********** write the data into sales team data mart **********")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                .select("store_id",
                        "sales_person_id",
                        "sales_person_first_name",
                        "sales_person_last_name",
                        "store_manager_name",
                        "manager_id",
                        "is_manager",
                        "sales_person_address",
                        "sales_person_pincode",
                        "sales_date",
                        "total_cost",
                        expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("********* final data for sales team data mart **********")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,
                                config.sales_team_data_mart_local_file)
logger.info(f"************* sales team data written at local disk at {config.sales_team_data_mart_local_file} *************")


#moving data on s3 bucket (sales datamart)

s3_directory= config.s3_sales_datamart_directory
message= s3_uploader.upload_to_s3(s3_directory,
                                  config.bucket_name,
                                  config.sales_team_data_mart_local_file)
logger.info(f"{message}")





#writing data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
                .option("header","true")\
                .mode("overwrite")\
                .partitionBy("sales_month","store_id")\
                .option("path",config.sales_team_data_mart_partitioned_local_file)\
                .save()

#move data on s3 for partitioned folder

s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp())*1000
for root,dirs,files in os.walk(config.sales_team_data_mart_partitioned_local_file):
        for file in files:
            print(file)
            local_file_path = os.path.join(root,file)
            relative_file_path = os.path.relpath(local_file_path,
                                     config.sales_team_data_mart_partitioned_local_file)
            s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
            s3_client.upload_file(local_file_path,config.bucket_name,s3_key)


#logger.info("******** calculating customer every month purchased amount **********")
#customer_mart_calculation_table_write(final_customer_data_mart_df)
#logger.info("*********calculation of customer mart done and written into table *********")
