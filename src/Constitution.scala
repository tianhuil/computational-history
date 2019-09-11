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

val inputFiles = Seq(
    "/mnt/volume_sfo2_02/downloads/google_ngrams/5/googlebooks-eng-us-all-5gram-20120701-ar.gz",
    "/mnt/volume_sfo2_02/downloads/google_ngrams/5/googlebooks-eng-us-all-5gram-20120701-ei.gz",
    "/mnt/volume_sfo2_02/downloads/google_ngrams/5/googlebooks-eng-us-all-5gram-20120701-ni.gz"
)

val data = spark.sparkContext.union(inputFiles.map(file => spark.read.textFile(file).rdd))

(data.filter(line => queries.exists(q => line.toLowerCase().contains(q)))
    .repartition(5)
    .saveAsTextFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5-part/constitution-sample"))
