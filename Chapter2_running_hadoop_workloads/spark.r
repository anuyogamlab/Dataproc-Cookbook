# Important: To use SparkR on Dataproc, require(SparkR) line imports the necessary library to work with Apache Spark from R.

require(SparkR)

# Initialize a Spark Session
sparkR.session(master = "yarn", appName = "SparkR Example")

data  <- list(
                     list(1L, "Anu","India"),
                     list(2L, "Ven","India"),
                     list(3L, "Gav","Canada")
                   )

schema <- structType(
                     structField("id",   "integer"),
                     structField("name", "string"),
                     structField("location", "string")
                   )

# Create SparkDataFrame
df     <- createDataFrame(
            data   = data,
            schema = schema
          )

# Print the schema
printSchema(df)

head(df)

#Stop the Spark Session
sparkR.stop()