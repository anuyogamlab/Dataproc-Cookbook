import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a SparkSession
spark = SparkSession.builder.appName("CloudSQLReader").master("yarn").getOrCreate()

# Replace with your Cloud SQL connection details
jdbcUrl = "jdbc:mysql://10.89.32.3:3306/enterprise"
username = "root"
password = ""
driverClass = "com.mysql.jdbc.Driver"

# Read data from Cloud SQL using JDBC
df = spark.read.format("jdbc").option("url", jdbcUrl).option("driver", driverClass).option("dbtable", "Persons").option("user", username).option("password", password).load()


# Print or save the results
df.show()

spark.stop()

