from pyspark.sql import *
from pyspark.sql.functions import col
import os
import time

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

# Finding users with lowest & highest numbers of average hours
if __name__ == "__main__":
    try:
        # Starting a spark session
        spark = SparkSession.builder.appName("pyspark01").master("local[3]").getOrCreate()
        sc = spark.sparkContext

        # Reading all the csv files in the stored directory
        df1 = spark.read.options(header=True, inferSchema=True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/cpulogsdb")
        # print(df1.describe().show())
        # print(df1.printSchema())
        print(df1.show(n=10, truncate=False, vertical=False))

        # Selecting only the required columns from the dataframe
        newdf1 = df1.select("DateTime", "user_name", "keyboard", "mouse")
        newdf1_filtered = newdf1.where((newdf1["keyboard"] != 0) & (newdf1["mouse"] != 0))
        newdf1_1 = newdf1_filtered.groupBy("user_name").count()
        print(newdf1_1.printSchema())
        newdf1_2 = newdf1_1.withColumn("Hours_spent", (col("count") * 5)/60).sort("Hours_spent")
        print(newdf1_2.show(n=10))
        print("Datatype of newdf1_2", type(newdf1_2))

        # Printing the output to terminal
        df1_pandas = newdf1_2.toPandas()
        print(df1_pandas.head(10))
        print("\n---------------------------------------------------------\n")
        print(f'The UserName with the lowest numbers of hours is :: {df1_pandas["user_name"][0] } with:: {df1_pandas["Hours_spent"][0] } hrs  \n'
              f'and the UserName with the highest numbers of hours is :: {df1_pandas["user_name"].iloc[-1]} with:: {df1_pandas["Hours_spent"].iloc[-1] } hrs')
        print("\n---------------------------------------------------------\n")
        print("==============================")

        # Writing the output dataframe to a local directory
        newdf1_2.coalesce(1).write.mode('overwrite').option("header", True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query01NotIdle")
        input("========================================= Press any KEY ======================================= ")

        # Pushing the output file to Azure Blob Storage
        basepath = 'C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query01NotIdle'
        filelist = os.listdir(basepath)
        ts = time.time()
        print(filelist[2])
        print("Azure Blob Storage v" + __version__ + " - Python")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container="blobdefault6", blob=f"query01-notIdle-{ts}.csv")
        with open(f"C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query01NotIdle/{filelist[2]}", "rb") as blob_file:
            blob_client.upload_blob(data=blob_file)
        print("success")

    except Exception as e:
        print(f"Error :: {e}")
        spark.stop()
    finally:
        spark.stop()
