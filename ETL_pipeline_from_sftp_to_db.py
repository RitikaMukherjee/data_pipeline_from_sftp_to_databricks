# Databricks notebook source
# DBTITLE 1,Install utilities
# MAGIC %pip install paramiko

# COMMAND ----------

# DBTITLE 1,Import
import paramiko
import stat# Import stat module for file type checking
import zipfile
from io import BytesIO
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# COMMAND ----------

# DBTITLE 1,Functions
def connect_sftp(hostname, username, password, port=22):
    try:
        transport = paramiko.Transport((host, port))#handling the SSH transport layer communication with the server
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)#Creates an SFTPClient instance from the Transport object, enabling file operations like upload/download
        print('SFTP connection is established')
        return sftp
    except Exception as e:
        print(f'Failed to connect to SFTP: {e}')
        return None
    

def list_matching_files(sftp, remote_path, pattern):
    try:
        files = sftp.listdir_attr(remote_path)  # Get file attributes along with names
        matching_files = [
            file.filename for file in files 
            if file.filename.startswith(pattern) and not stat.S_ISDIR(file.st_mode)#Ensures the item is a file, not a directory
        ]
    except Exception as e:
        print(f'Error in listing files: {e}')
        return []

# COMMAND ----------

# DBTITLE 1,Spark session
#initialize spark session
spark=SparkSession.builder.appName("SFTP to Databricks").config("spark.databricks.delta.schema.autoMerge.enabled", "true").getOrCreate()
#When writing data to a Delta Table, if the schema changes (e.g., new columns are added), Spark will automatically merge the schema without failing

# COMMAND ----------

# DBTITLE 1,SFTP connection
hostname='localhost'
username='ritika'
password='Ritika123'
# port=22
sftp=connect_sftp(hostname, username, password)

# COMMAND ----------

# DBTITLE 1,Create folder paths
databricks_zipped_folder="/FileStore/tables/zipped/"
databricks_unzipped_folder="/FileStore/tables/unzipped/"
databricks_archive_folder="/FileStore/tables/archived/"
remote_path="/"
sftp_archive_path="/C:/SFTP/archive/"

# COMMAND ----------

# DBTITLE 1,Create pipeline
if sftp:
    try:
        matching_files=list_matching_files(sftp, remote_path, "test_monthly")
        print("Matching files: ", matching_files)

        if matching_files:
            for file_name in matching_files:
                remote_file_path=f'{remote_path}{file_name}'
                databricks_file_path_zipped=f'{databricks_zipped_folder}{file_name}'
                databricks_file_path_archived=f'{databricks_archived_folder}{file_name}'
                sftp_archive_file_path=f'{sftp_archive_path}{file_name}'

                try:
                    #read the zip file from sftp
                    with sftp.open(remote_file_path, mode='rb') as remote_file:#Opens the file from SFTP in binary mode
                        file_content= remote_file.read()

                        #write the zip file directly to databricks zipped folder
                        dbutils.fs.put(databricks_file_path_zipped, file_content.decode('latin1'),overwrite=True)
                        print(f'File {file_name} successfully saved to {databricks_file_path_zipped}')

                        #if it is a zipped file extract it and save the content
                        if file_name.endswith('.zip'):
                            print(f'Extracting Zip files: {file_name}')
                            with zipfile.ZipFile(io.BytesIO(file_content), 'r') as zip_ref:
                                for extracted_file in zip_ref.namelist():
                                    unzipped_content= zip_ref.read(extracted_file)
                                    unzipped_file_path= f'{databricks_unzipped_folder}{extracted_file}'
                                    
                                    #save the extracted file to databricks unzipped folder
                                    text_content= unzipped_content.decode('utf-8', errors= 'ignore')
                                    dbutils.fs.put(unzipped_file_path,text_content,overwrite=True)
                                    print(f'Unzipped file {extracted_file} is saved to {unzipped_file_path}')

                                    #read the unzipped file into a spark dataframe
                                    df= spark.read.option('delimeter', '|').option('header','true')\
                                        .option('inferschema', 'true').csv(unzipped_file_path)

                                    #rename column to remove spaces and special character
                                    df= df.toDF(*(col_name.strip().replace(" ","_").replace("(","").replace(")","").replace("|","_").replace("\n","") for col_name in df.columns))

                                    #add filename and fileloaddate column
                                    file_load_date = datetime.now().strftime("%Y-%m-%d")
                                    df= df.withColumn('FileName', lit(file_name))
                                    df= df.withColumn('FileLoadDate', lit(file_load_date))

                                    #defile table names
                                    table_name= f'Payer_{os.path.splitext(extracted_file)[0]}'
                                    stg_table=f'col.stg.{table_name}'
                                    dbo_table=f'col.dbo.{table_name}'

                                    #overwrite in staging table
                                    df.write.mode("Overwrite").format("delta").option("mergeSchema","true").saveAsTable(stg_table)
                                    print(f'Staging table {stg_table} is created')

                                    #append to final table
                                    if spark.catalog.tableExists(dbo_table):
                                        existing_df= spark.read.format("delta").saveAsTable(dbo_table)
                                        #combine new and existing data and drop duplicates
                                        combined_df= existing_df.union(df).dropDuplicates()

                                        #check if new record exists before overwriting
                                        if combined_df.count()>existing_df.count():
                                            combined_df.write.mode("overwrite").format("delta").saveAsTable(dbo_table)
                                            print(f'distinct records updated in {dbo_table}')
                                        else:
                                            print(f"no new record to print in {dbo_table}")
                                    else:
                                        df.write.mode("overwrite").format("delta").saveAsTable(dbo_table)
                                        print(f"Final table {dbo_table} is created")
                                    
                                    #delete file from unzipped folder
                                    dbutils.fs.rm(unzipped_file_path, True)
                                    print(f"Deleted {unzipped_file_path} from unzipped folder")
                            
                        #Move the zip file to databricks archive
                        dbutils.fs.mv(databricks_file_path_zipped, databricks_file_path_archived)
                        print(f"Moved {file_name} to archive folder")

                        #Move the zip file to sftp archive
                        sftp.rename(remote_file_path,sftp_archive_file_path)
                        print(f"Moved {file_name} to sftp archive: {sftp_archive_file_path}")

                        print("load is successful")

                except Exception as e:
                    print(f"Error processing {file_name}: {e}")
                    continue
        else:
            print("No matching files found")
    except Exception as e:
        print(f"error listing files: {e}")

    finally:
        sftp.close()

else:
    print("Failed to connect SFTP")
                            
