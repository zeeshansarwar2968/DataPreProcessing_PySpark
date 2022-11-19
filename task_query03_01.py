from pyspark.sql import *
from pyspark.sql.functions import col, row_number
import os,time
from pyspark.sql.window import Window
from pyspark.sql.functions import *

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
# Finding users with highest numbers of times late comings

if __name__ == "__main__":
    try:
        # Starting a spark session
        spark = SparkSession.builder.appName("pyspark01").master("local[3]").getOrCreate()
        sc = spark.sparkContext

        # Reading all the csv files in the stored directory
        df1 = spark.read.options(header=True, inferSchema=True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/cpulogsdb")
        print(df1.show(n=10, truncate=False, vertical=False))

        # Selecting only the required columns from the dataframe
        df1_01 = df1.select("DateTime", "Cpu Working Time", "Cpu idle Time", "user_name", "keyboard", "mouse")
        print(df1_01.printSchema())
        print(df1_01.show(n=10))

        df1_iterm = df1_01.withColumn("time", date_format("DateTime", "HH:mm")).sort("time")
        print(df1_iterm.show(n=100))
        print(df1_iterm.printSchema())

        # Partitioning and filtering to gain first datetime values of each user
        w2 = Window.partitionBy("user_name").orderBy(col("user_name"))
        df1_02 = df1_iterm.withColumn("row", row_number().over(w2)) .filter(col("row") == 1).drop("row").sort("time")
        print(df1_02.show())

        # Taking the lowest datetime value and inserting into the working dataframe
        df1_date = df1_02.first()["DateTime"]
        print("df1 data :: ", df1_date)
        df1_02_wip = df1_02.withColumn("initialDateTime", lit(df1_date))
        df1_03 = df1_02_wip.withColumn("time_initial", date_format("initialDateTime", "HH:mm"))
        print(df1_03.show())
        print(df1_03.printSchema())

        # Subtracting each user time value with the initial value
        df2_01 = df1_03.withColumn('from_time', to_timestamp(col('time_initial'))) \
            .withColumn('end_time', to_timestamp(col('time'))) \
            .withColumn('DiffInSeconds', col("end_time").cast("long") - col('from_time').cast("long"))
        print(df2_01.show(truncate=False))

        # Printing the late coming count in descending order
        df2_02 = df2_01.withColumn("LateComingsCount", (col("DiffInSeconds")/300)).sort(desc("DiffInSeconds"))
        df_final = df2_02.select("user_name", "LateComingsCount")
        print("\n ------------------***********************************-------------------\n")
        print(df_final.show())
        print("\n ------------------***********************************-------------------\n")

        # Printing the output to terminal
        df_pandas = df_final.toPandas()
        print(df_pandas.head(10))
        print("\n ------------------***********************************-------------------\n")
        print(f'The UserName with the lowest numbers of late comings is :: {df_pandas["user_name"].iloc[-1] } with count :: {df_pandas["LateComingsCount"].iloc[-1] }   \n'
             f'and the UserName with the highest numbers of late comings is :: {df_pandas["user_name"].iloc[0]} with count :: {df_pandas["LateComingsCount"].iloc[0] } ')
        print("\n ------------------***********************************-------------------\n")

        # Writing the output dataframe to a local directory
        df2_02.coalesce(1).write.mode('overwrite').option("header", True).csv(
            "C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query03-R02")
        input("========================================= Press any KEY ======================================= ")

        # Pushing the output file to Azure Blob Storage
        basepath = 'C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query03-R02'
        filelist = os.listdir(basepath)
        ts = time.time()
        print(filelist[2])
        print("Azure Blob Storage v" + __version__ + " - Python")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(container="blobdefault6", blob=f"query03-R02-{ts}.csv")
        with open(f"C:/Users/zeesh/Documents/AzureDataEngineer/outputs/query03-R02/{filelist[2]}", "rb") as blob_file:
            blob_client.upload_blob(data=blob_file)
        print("success")
    except Exception as e:
        print(f"Error :: {e}")
        spark.stop()
    finally:
        spark.stop()