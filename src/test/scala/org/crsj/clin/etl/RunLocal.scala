package org.crsj.clin.etl

import org.apache.spark.sql.SparkSession

object RunLocal extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config("es.nodes", System.getenv.getOrDefault("es.nodes", "localhost"))
    .config("es.port", System.getenv.getOrDefault("es.port", "9200"))
    .appName("Index Fhir")
    .getOrCreate()


  private val base = System.getenv.getOrDefault("input.directory", "./ndjson")

  ETL.run(base)


}
