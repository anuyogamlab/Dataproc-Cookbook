from google.cloud import dlp_v2
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf, col

# Initialize Spark session
spark = SparkSession.builder.appName("DLPDeID").getOrCreate()

df = spark.read.parquet("gs://nstestb/customer_data/")
df.show()


# Function to de-identify first_name and last_name using DLP template
def deidentify_names(record, project_id, template_id):
    dlp_client = initialize_dlp_client()

    # Construct template name manually
    template_name = f"projects/{project_id}/deidentifyTemplates/{template_id}"

    # Create the content item with first_name and last_name for de-identification
    item = {"table": {
        "headers": [{"name": "first_name"}, {"name": "last_name"}],
        "rows": [{
            "values": [{"string_value": record['first_name']}, {"string_value": record['last_name']}]
        }]
    }}

    # Prepare the Deidentify request
    request = {
        "parent": f"projects/{project_id}",
        "deidentify_template_name": template_name,
        "item": item,
    }

    # Call the DLP API
    response = dlp_client.deidentify_content(request)

    # Extract pseudonymized values from the response
    pseudonymized_first_name = response.item.table.rows[0].values[0].string_value
    pseudonymized_last_name = response.item.table.rows[0].values[1].string_value

    return pseudonymized_first_name, pseudonymized_last_name


# Define a UDF to call DLP API
def pseudonymize(first_name, last_name):
    pseudonymized_fn, pseudonymized_ln = deidentify_names({"first_name": first_name, "last_name": last_name}, project_id, template_id)
    return pseudonymized_fn , pseudonymized_ln

# Register the UDF
pseudonymize_udf = udf(pseudonymize, StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType())
]))


# Apply the UDF to your DataFrame and create separate columns
df = df.withColumn("pseudonymized_names", pseudonymize_udf(col("first_name"), col("last_name"))) \
    .select(
    "*",  # Select all existing columns
    col("pseudonymized_names.first_name").alias("pseudonymized_first_name"),
    col("pseudonymized_names.last_name").alias("pseudonymized_last_name")
).drop("pseudonymized_names")  # Optionally drop the struct column

# Show the results
df.show(truncate=False)