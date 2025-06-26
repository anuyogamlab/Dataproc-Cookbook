import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

# Create a SparkSession (entry point to Spark functionality)

spark = SparkSession.builder.appName("wikiinsights") \
    .master("yarn") \
    .getOrCreate()

#Get the input path and output
inputPath = sys.argv[1]
outputTable = sys.argv[2]

print("Reading the CSV input file")

# Read the CSV, inferring the schema and assuming a header row

df = spark.read.option("header", True).csv(inputPath)


# Perform aggregation: Group by 'language' and get counts

outputdf=df.groupBy("language").count()


# Write the aggregated DataFrame to BigQuery and “direct” mode doesn’t write intermediate files

outputdf.write \
    .format("bigquery") \
    .option("writeMethod", "direct") \
    .mode("append") \
    .save(outputTable)

print("Application Completed!!!")

# Closing the Spark session
spark.stop()