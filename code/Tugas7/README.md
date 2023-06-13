<br />
<div align="center">
<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Pengumpulan tugas Spark Big Data Tugas 7
  </p>
</div>

### Code 1
```sh
!pip install pyspark
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import math

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

class Rating:
    def __init__(self, userId, movieId, rating, timestamp):
        self.userId = userId
        self.movieId = movieId
        self.rating = rating
        self.timestamp = timestamp

def parseRating(line):
    fields = line.split("::")
    assert len(fields) == 4
    return Rating(int(fields[0]), int(fields[1]), float(fields[2]), int(fields[3]))

# Test it
parseRating("1::1193::5::978300760")

raw = sc.textFile("ratings.dat")
# check one record. it should be res4: Array[String] = Array(1::1193::5::978300760)
# If this fails the location of the file is wrong.
raw.take(1)

ratings = raw.map(parseRating).toDF()
# check if everything is ok
ratings.show(5)

training, test = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Alternating Least Squares (ALS) matrix factorization.
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")

model = als.fit(training)
model.save("mymodel")

# Prepare the recommendations
predictions = model.transform(test)
squared_diff = predictions.select((col("rating").cast("float") - col("prediction").cast("float")).alias("squared_diff")).na.drop()
squared_diff_squared = squared_diff.select((col("squared_diff") ** 2).alias("squared_diff_squared")).na.drop()
mse = squared_diff_squared.agg({"squared_diff_squared": "mean"}).collect()[0][0]
rmse = math.sqrt(mse)

predictions.take(10)

predictions.write.format("com.databricks.spark.csv").save("ml-predictions.csv")
```

Screenshot: 


Penjelasan code
```sh

```

### Code 2
```sh
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

# Inisialisasi sesi Spark
spark = SparkSession.builder.appName("GoogleColabSpark").getOrCreate()

lines = spark.sparkContext.textFile("ratings.dat")
parts = lines.map(lambda row: row.split("::"))

# Konversi RDD ke DataFrame
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=int(p[2]), timestamp=int(p[3])))
ratings = spark.createDataFrame(ratingsRDD)

# Split data menjadi training dan test set
(training, test) = ratings.randomSplit([0.8, 0.2])

# Build the recommendation model using ALS on the training data
# Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
predictions.show()

import math
result = predictions.rdd.map(lambda row: row['prediction'] - row['rating']).map(lambda x: x*x).filter(lambda x: not math.isnan(x))
```
Screenshot: 


Penjelasan code
```sh

```

### Code 3
```sh
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

mat = sc.parallelize([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])

summary = Statistics.colStats(mat)
print("Mean:", summary.mean())
print("Variance:", summary.variance())
print("Number of Nonzeros:", summary.numNonzeros())
```
Screenshot: 


Penjelasan code
```sh

```

### Code 4
```sh
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

mat = sc.parallelize([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])

summary = Statistics.colStats(mat)
print("Mean:", summary.mean())
print("Variance:", summary.variance())
print("Number of Nonzeros:", summary.numNonzeros())
```
Screenshot: 


Penjelasan code
```sh

```