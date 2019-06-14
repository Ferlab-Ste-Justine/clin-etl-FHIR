package org.crsj.clin.etl

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}

object Specimen {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/sp.ndjson", $"id", $"subject", $"status", $"request.id" as "request", expr("container.identifier[0].value") as "container")
  }
}
