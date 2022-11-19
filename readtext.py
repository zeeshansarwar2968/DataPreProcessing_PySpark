from pyspark.sql import *
import matplotlib
import os
import shutil
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')

if __name__ == "__main__":

    spark = SparkSession.builder.appName("pyspark01").master("local[3]").getOrCreate()
    sc = spark.sparkContext
    rdd = sc.textFile("C:/Users/zeesh/Documents/AzureDataEngineer/cpulogsdb/*.csv")
    rdd_data = rdd.collect()
    count = 0
    print(type(rdd_data))
    print(type(rdd_data[1]))
    print(rdd_data[1][:19])

    for i in rdd_data:
        # print(i[1])
        count += 1
    print(count)
    spark.stop()
