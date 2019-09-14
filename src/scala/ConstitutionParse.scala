import org.apache.hadoop.io.compress.GzipCodec

val debug = false
val limit = 100000

val file = spark.read.textFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5/constitution-gz/*.gz")

val data = if (debug) {
    file.limit(limit)
} else {
    file
}

case class NGramRow(ngram: String, year: String, total: String, distinct: String)
object NGramRow {
    def parse(line: String): Option[NGramRow] = {
        line.trim.split('\t') match {
            case Array(ngram, year, total, distinct) => {
                Some(new NGramRow(ngram.toLowerCase, year, total, distinct))
            }
            case _ =>  None
        }
    }
}

val matchersBV = spark.sparkContext.broadcast(queries.map(q => s"\\b$q\\b".r))

val noQuery = spark.sparkContext.longAccumulator("No Query")
val multipleQueries = spark.sparkContext.longAccumulator("Multiple Queries")
val strangeRemainders = spark.sparkContext.longAccumulator("Strange Remainders")
val valid = spark.sparkContext.longAccumulator("Valid")

case class Row(query: String, remainders: List[String], year: String, total: String, distinct: String)
object RowObj {
    def parse(row: NGramRow): Option[Row] = {
        val matches = for {
            matcher <- matchersBV.value
            match_ <- matcher.findAllIn(row.ngram).toList
        } yield {
            match_
        }
        matches match {
            case query :: Nil => {
                row.ngram.replaceAll(query, "").trim.split(" ").filter(_ != "") match {
                    case Array(r1, r2, r3) => {
                        valid.add(1)
                        Some(Row(query, List(r1, r2, r3), row.year, row.total, row.distinct))
                    }
                    case arr => {
                        strangeRemainders.add(1)
                        if (debug) Some(Row(query, arr.toList, row.year, row.total, row.distinct)) else None
                    }
                }
            }
            case Nil => {
                noQuery.add(1)
                if (debug) Some(Row(row.ngram, Nil, row.year, row.total, row.distinct)) else None
            }
            case _ => {
                multipleQueries.add(1)
                None
            }
        }
    }

    def toTSV(row: Row): String = {
        (List(row.query) ++ row.remainders.toList ++ List(row.year, row.total, row.distinct)).mkString("\t")
    }
}

val parsed = (data.rdd
    .flatMap(NGramRow.parse _)
    .flatMap(RowObj.parse _))

val result = if (debug) {
    (parsed.collect())
} else {
    (parsed.map(RowObj.toTSV _)
        .repartition(1)
        .saveAsTextFile("/mnt/volume_sfo2_03/downloads/google_ngrams/5/constitution-parsed-gz/", classOf[GzipCodec]))
}
