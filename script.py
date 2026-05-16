from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import  StringType
from pyspark.sql import functions as F
import os
import sys
os.environ["JAVA_HOME"] = "/lib/jvm/java-11-openjdk-amd64"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession \
    .builder \
    .appName("testeKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()\

df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","testSparking")\
    .load()

df_final = df.selectExpr("CAST(value AS STRING) as pessoa")\
    .withColumn("cpf", F.when(F.col("pessoa") =="pedro","123.098.456-87").
                otherwise("123.098.456-86"))

query = df_final.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
