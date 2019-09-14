import org.apache.hadoop.io.compress.GzipCodec

val data = spark.read.textFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5-part/*.gz").rdd

(data.filter(line => queries.exists(q => line.toLowerCase().contains(q)))
    .repartition(10)
    .saveAsTextFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5/constitution-gz", classOf[GzipCodec]))
