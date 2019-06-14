package org.crsj.clin.etl

import org.apache.spark.sql.SparkSession

object Main extends App {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Index Fhir")
    .getOrCreate()

  ETL.run("/ndjson")


}
