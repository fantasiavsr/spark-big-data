<br />
<div align="center">
<h3 align="center">Spark Big Data</h3>

  <p align="center">
    Pengumpulan tugas Spark Big Data minggu 8
  </p>
</div>

## Spark Streaming
### Code 1
```sh
# Execute below code with command, 
# spark-submit --master local[*] network_wordcount.py localhost 9999

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


```
Screenshot: 
![image](https://github.com/fantasiavsr/spark-big-data/blob/master/code/Chapter5/00_images/1.png)

Penjelasan code
```sh
sys.argv:	sys.argv adalah list pada python yang berisi semua perintah pada command-line
sys.stderr:	sys.stderr mencetak langsung ke konsol berupa pesan pengecualian (exception) dan kesalahan
StreamingContext:	StreamingContext mewakili koneksi ke cluster Spark dan dapat digunakan untuk membuat berbagai sumber input DStream
sc:	Default dari PySpark SparkContext
socketTextStream:	Buat input dari nama host sumber TCP: port. Data diterima menggunakan soket TCP dan menerima byte ditafsirkan sebagai UTF8 yang disandikan \n baris yang dibatasi.

```

