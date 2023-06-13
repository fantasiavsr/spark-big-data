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

#### Screenshot:
![image](https://github.com/fantasiavsr/spark-big-data/assets/86558365/2ca72f51-1296-418f-91a9-90f6c0caee59)


#### Penjelasan code:
Mengimplementasikan sistem rekomendasi menggunakan teknik ALS (Alternating Least Squares) pada data peringkat film. Pertama, kelas "Rating" didefinisikan dengan atribut-atribut seperti ID pengguna, ID film, peringkat, dan timestamp. Fungsi "parseRating" digunakan untuk memisahkan baris data dengan pemisah "::" dan menghasilkan objek Rating. Kemudian, data mentah dibaca dari file "ratings.dat" dan diproses menggunakan metode "map" untuk mengonversi setiap baris menjadi objek Rating, kemudian diubah menjadi DataFrame. Data dibagi menjadi set pelatihan (80%) dan set pengujian (20%). Model rekomendasi dibangun menggunakan ALS pada data pelatihan dengan parameter seperti jumlah iterasi, regParam, kolom pengguna, kolom item, dan kolom peringkat. Model yang telah dilatih disimpan dalam file "mymodel". Prediksi dilakukan pada data pengujian menggunakan model yang dilatih, dan selisih kuadrat antara peringkat yang diprediksi dan peringkat sebenarnya dihitung. Kemudian, nilai MSE (Mean Squared Error) dan RMSE (Root Mean Squared Error) dihitung sebagai metrik evaluasi model. Hasil prediksi ditulis dalam format CSV.


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
#### Screenshot: 
![image](https://github.com/fantasiavsr/spark-big-data/assets/86558365/9a68f25f-62f6-4b31-b704-f944a37e7eec)


#### Penjelasan code:
Apache Spark untuk membangun sistem rekomendasi dengan menggunakan ALS (Alternating Least Squares). Pertama, sesi Spark diinisialisasi dengan nama "GoogleColabSpark". Data dari file "ratings.dat" dibaca sebagai RDD (Resilient Distributed Dataset) dan dipisahkan menggunakan delimiter "::". Selanjutnya, RDD diubah menjadi DataFrame dengan atribut-atribut seperti userId, movieId, rating, dan timestamp. Data tersebut kemudian dibagi menjadi set pelatihan dan set pengujian. Model rekomendasi ALS dengan parameter seperti jumlah iterasi, regParam, dan kolom-kolom yang sesuai, diinisialisasi. Model tersebut dilatih dengan menggunakan data pelatihan. Prediksi dilakukan pada data pengujian dengan menggunakan model yang dilatih, dan hasil prediksi ditampilkan. Selanjutnya, dilakukan evaluasi model dengan menghitung RMSE (Root Mean Squared Error) pada data pengujian. RMSE dihitung dengan mengurangi peringkat yang diprediksi dengan peringkat sebenarnya, kemudian mengkuadratkannya dan menghilangkan nilai-nilai NaN. Hasil evaluasi tersebut dapat digunakan untuk mengukur performa model rekomendasi.


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
#### Screenshot: 
![image](https://github.com/fantasiavsr/spark-big-data/assets/86558365/a2bdd651-6aa7-4a6c-a0dc-ea9f128981fe)


#### Penjelasan code:
Apache Spark untuk melakukan analisis statistik pada data matriks. Pertama, sesi Spark diinisialisasi dengan menggunakan SparkSession. Selanjutnya, objek SparkContext (sc) diinisialisasi dari sesi Spark. Data matriks (mat) dibentuk sebagai RDD dengan menggunakan metode "parallelize" dengan elemen-elemen sebagai array 2 dimensi. Kemudian, metode "colStats" dari modul "Statistics" digunakan untuk menghitung statistik kolom pada matriks tersebut. Objek summary akan berisi ringkasan statistik seperti mean (rata-rata), variance (variansi), dan numNonzeros (jumlah elemen non-nol) dari setiap kolom matriks. Hasil statistik tersebut kemudian ditampilkan menggunakan pernyataan "print".
