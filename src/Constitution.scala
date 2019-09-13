import org.apache.hadoop.io.compress.GzipCodec

val articles = List(
    "article one",
    "article two",
    "article three",
    "article four",
    "article five",
    "article six",
    "article seven"
)

val amendments = List(
    "first amendment",
    "second amendment",
    "third amendment",
    "fourth amendment",
    "fifth amendment",
    "sixth amendment",
    "seventh amendment",
    "eighth amendment",
    "ninth amendment",
    "tenth amendment",
    "eleventh amendment",
    "twelfth amendment",
    "thirteenth amendment",
    "fourteenth amendment",
    "fifteenth amendment",
    "sixteenth amendment",
    "seventeenth amendment",
    "eighteenth amendment",
    "nineteenth amendment",
    "twentieth amendment",
    "twenty-first amendment",
    "twenty-second amendment",
    "twenty-third amendment",
    "twenty-fourth amendment",
    "twenty-fifth amendment",
    "twenty-sixth amendment",
    "twenty-seventh amendment"
)

val queries = articles ++ amendments

val data = spark.read.textFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5-part/*.gz").rdd

(data.filter(line => queries.exists(q => line.toLowerCase().contains(q)))
    .repartition(10)
    .saveAsTextFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5/constitution-gz", classOf[GzipCodec]))
