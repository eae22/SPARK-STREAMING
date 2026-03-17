from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, current_timestamp

spark = SparkSession.builder \
    .appName("WordCountFile") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = "value STRING"

lines = spark.readStream \
    .format("text") \
    .schema(schema) \
    .option("path", "/app/data") \
    .load()

words = lines.select(
    explode(split(col("value"), " ")).alias("word"),
    current_timestamp().alias("timestamp")
)

word_count = words.groupBy("word").count().orderBy("count", ascending=False)

query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

