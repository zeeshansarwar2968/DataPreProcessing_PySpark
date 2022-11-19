from pyspark.sql import *
from pyspark.sql.functions import col
import os
import shutil
import time

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Finding users with highest numbers of times late comings & idle hours

if __name__ == "__main__":

        spark = SparkSession.builder.appName("pyspark01").master("local[3]").getOrCreate()
        sc = spark.sparkContext
        df1 = spark.read.options(header=True, inferSchema=True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/cpulogsdb")
        print(df1.show(n=10, truncate=False, vertical=False))
        df1_01 = df1.select("DateTime", "Cpu Working Time", "Cpu idle Time", "user_name", "keyboard", "mouse")
        # print(df1_01.show(n=10))
        df1_idle = df1_01.filter((df1_01["keyboard"] == 0.0) & (df1_01["mouse"] == 0.0))
        print(df1_idle.show(n=10))
        df1_02 = df1_idle.groupBy("user_name").count()
        df1_03 = df1_02.withColumn("Hours_Idle", (col("count") * 5)/60).sort("Hours_Idle")
        print(df1_03.show(n=10))
        df1_pandas = df1_03.toPandas()
        print(df1_pandas.head(10))
        print("\n---------------------------------------------------------\n")
        print(f'The UserName with the lowest numbers of idle hours is :: {df1_pandas["user_name"][0] } with:: {df1_pandas["Hours_Idle"][0] } hrs  \n'
             f'and the UserName with the highest numbers of idle hours is :: {df1_pandas["user_name"].iloc[-1]} with:: {df1_pandas["Hours_Idle"].iloc[-1] } hrs')
        print("\n---------------------------------------------------------\n")
        df1_03.coalesce(1).write.mode('overwrite').option("header", True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query02")
        # input("========================================= Press any KEY ======================================= ")

        basepath = 'C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query02'
        filelist = os.listdir(basepath)
        ts = time.time()
        print(filelist[2])
        print("Azure Blob Storage v" + __version__ + " - Python")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container="blobdefault6", blob=f"query02-{ts}.csv")
        with open(f"C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query02/{filelist[2]}", "rb") as blob_file:
            blob_client.upload_blob(data=blob_file)
        # blob_client.upload_blob(data=blob_file)
        print("success")

        spark.stop()