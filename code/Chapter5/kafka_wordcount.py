from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Buat session Spark
spark = SparkSession.builder \
    .appName("KafkaWordCount") \
    .getOrCreate()

# Baca data dari Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic1") \
    .load()

# Transformasi data
words = df.selectExpr("CAST(value AS STRING)") \
    .select(split("value", " ").alias("words")) \
    .selectExpr("explode(words) as word")

# Hitung jumlah kata
wordCounts = words.groupBy("word").count()

# Menampilkan hasil
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Menunggu hingga proses selesai
query.awaitTermination()
