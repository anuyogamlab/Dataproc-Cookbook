import org.apache.spark.sql.{SparkSession, SaveMode}

object XmlToParquet {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new IllegalArgumentException("Usage: <input-xml-path> <output-parquet-path> <root-tag>")
    }

    val spark = SparkSession.builder()
      .appName("XmlToParquet")
      .getOrCreate()

    val xmlDataFrame = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", args(2))
      .load(args(0))

    xmlDataFrame.write
      .mode(SaveMode.Overwrite)
      .parquet(args(1))

    spark.stop()
  }
}
