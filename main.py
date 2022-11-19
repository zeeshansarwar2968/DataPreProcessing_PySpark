from pyspark.sql import *
import os
import shutil
import time

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

if __name__ == "__main__":

    spark = SparkSession.builder.appName("pyspark01").master("local[3]").getOrCreate()
    sc = spark.sparkContext
    df1 = spark.read.options(header=True, inferSchema=True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/cpulogsdb")
    print(df1.describe().show())
    pandasdf1 = df1.toPandas()
    print("==============================")
    print(pandasdf1.describe())
    print("==============================")
    print(pandasdf1.isna())
    print("==============================")
    df1.coalesce(1).write.mode('overwrite').option("header", True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/outputs/csvout")
    input("========================================= Press any KEY ======================================= ")
    spark.stop()
    #################

    basepath = 'C:/Users/zeesh/Documents/AzureDataEngineer/outputs/csvout'
    filelist = os.listdir(basepath)
    ts = time.time()
    print(filelist[2])
    print("Azure Blob Storage v" + __version__ + " - Python")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container="blobdefault6", blob=f"outputfile-{ts}.csv")
    with open(f"C:/Users/zeesh/Documents/AzureDataEngineer/outputs/csvout/{filelist[2]}", "rb") as blob_file:
        blob_client.upload_blob(data=blob_file)
    # blob_client.upload_blob(data=blob_file)
    print("success")















