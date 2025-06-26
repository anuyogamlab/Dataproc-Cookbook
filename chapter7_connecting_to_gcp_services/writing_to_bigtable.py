from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BigtableExample") \
    .getOrCreate()

bigtable_project_id = "dataproc-cookbook-425300"
bigtable_instance_id = "test-instance"
bigtable_table_name = "wordinsights"

catalog = ''.join(("""{
        "table":{"name":" """ + bigtable_table_name + """
        ", "tableCoder":"PrimitiveType"},
        "rowkey":"wordCol",
        "columns":{
          "word":{"cf":"rowkey", "col":"wordCol", "type":"string"},
          "count":{"cf":"example_family", "col":"countCol", "type":"long"}
        }
        }""").split())

csv_file_path = "gs://bucket_name/sample.csv"
input_data = spark.read.csv(csv_file_path, header=True, inferSchema=True)

input_data.write \
    .format('bigtable') \
    .options(catalog=catalog) \
    .option('spark.bigtable.project.id', bigtable_project_id) \
    .option('spark.bigtable.instance.id', bigtable_instance_id) \
    .option('spark.bigtable.create.new.table', "false") \
    .save()
