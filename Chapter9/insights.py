import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("wikiinsights") \
    .master("yarn") \
    .getOrCreate()

#Get the input path and output path
inputPath = sys.argv[1]
outputPath = sys.argv[2]

print("Reading the CSV input file")

df = spark.read.csv(inputPath,inferSchema=True,header=True)

df.groupBy("language").count().write.mode("overwrite").parquet(outputPath)
    
print("Application Completed!!!")

# Closing the Spark session
spark.stop()
