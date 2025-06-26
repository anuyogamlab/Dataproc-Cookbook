import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("XmltoParquet") \
    .master("yarn") \
    .getOrCreate()

#Get the input path, output path and rowTag
inputPath = sys.argv[1]
outputPath = sys.argv[2]
rowTag = sys.argv[3]

print("Reading the XML input file")

df = spark.read.format('xml').option("rowTag", rowTag).load(inputPath)

# Show the DataFrame to check the data
df.write.mode("overwrite").parquet(outputPath)

print("Application Completed!!!")

# Closing the Spark session
spark.stop()