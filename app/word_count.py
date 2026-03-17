from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

lines = spark.readStream \
    .format("socket") \
    .option("host", "host.docker.internal") \
    .option("port", 9999) \
    .load()

words = lines.select(
    explode(split(col("value"), " ")).alias("word")
)

word_count = words.groupBy("word").count().orderBy("count", ascending=False)

query = word_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()