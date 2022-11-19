from pyspark.sql import *
from pyspark.sql.functions import col, row_number
import os
from pyspark.sql.window import Window
from pyspark.sql.functions import *


# Finding users with highest numbers of times late comings & idle hours

if __name__ == "__main__":
    # try:
        spark = SparkSession.builder.appName("pyspark01").master("local[3]").getOrCreate()
        sc = spark.sparkContext
        df1 = spark.read.options(header=True, inferSchema=True).csv("C:/Users/zeesh/Documents/AzureDataEngineer/cpulogsdb")
        print(df1.show(n=10, truncate=False, vertical=False))
        df1_01 = df1.select("DateTime", "Cpu Working Time", "Cpu idle Time", "user_name", "keyboard", "mouse")
        print(df1_01.printSchema())
        print(df1_01.show(n=10))
        w2 = Window.partitionBy("user_name").orderBy(col("user_name"))
        df1_02 = df1_01.withColumn("row", row_number().over(w2)) .filter(col("row") == 1).drop("row")
        print(df1_02.show())
        # split_col = F.split(df1_01['DateTime'], ' ')
        # df_date = df1_01.withColumn('Date', split_col.getItem(0))
        # df_time = df1_01.withColumn('Time', F.concat(split_col.getItem(1), F.lit(' '), split_col.getItem(2)))
        # print(df_time.show(n=10))
        # df1_datetime = df1_01.select("DateTime")
        # print(df1_datetime.show(n=10))
        df1_date = df1_01.first()["DateTime"]
        print("df1 data :: ", df1_date)
        df1_03 = df1_02.withColumn("initialTime", lit(df1_date))
        print(df1_03.show())

# timeStamp01 = spark.sql("select to_timestamp('2019-09-19 08:40:02') as timestamp")
        # print(type(timeStamp01))
        # print(timeStamp01.show())
        # print(timeStamp01.printSchema())
        df2_01 = df1_03.withColumn('from_timestamp', to_timestamp(col('initialTime'))) \
            .withColumn('end_timestamp', to_timestamp(col('DateTime'))) \
            .withColumn('DiffInSeconds', col("end_timestamp").cast("long") - col('from_timestamp').cast("long"))
        print(df2_01.show(truncate=False))

        df2_02 = df2_01.withColumn("LateComingsCount", (col("DiffInSeconds")/300)).sort(desc("DiffInSeconds"))
        df_final = df2_02.select("user_name", "LateComingsCount")
        print("\n ------------------***********************************-------------------\n")
        print(df_final.show())
        print("\n ------------------***********************************-------------------\n")
        df_pandas = df_final.toPandas()
        print(df_pandas.head(10))

# df1_idle = df1_01.filter((df1_01["keyboard"] == 0.0) & (df1_01["mouse"] == 0.0))
        # print(df1_idle.show(n=10))
        # df1_02 = df1_idle.groupBy("user_name").count()
        # df1_03 = df1_02.withColumn("Hours_Idle", (col("count") * 5)/60).sort("Hours_Idle")
        # print(df1_03.show(n=10))
        # df1_pandas = df1_03.toPandas()
        # print(df1_pandas.head(10))
        # print("\n---------------------------------------------------------\n")
        # print(f'The UserName with the lowest numbers of idle hours is :: {df1_pandas["user_name"][0] } with:: {df1_pandas["Hours_Idle"][0] } hrs  \n'
        #      f'and the UserName with the highest numbers of idle hours is :: {df1_pandas["user_name"].iloc[-1]} with:: {df1_pandas["Hours_Idle"].iloc[-1] } hrs')
        # print("\n---------------------------------------------------------\n")
        # input("========================================= Press any KEY ======================================= ")
    # except Exception as e:
    #     print(f"Error :: {e}")
    #     spark.stop()
    # finally:
        spark.stop()