package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object ServiceRequest {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/sr.ndjson", $"id", $"status", $"intent", $"authoredOn", $"code", $"subject")
  }
}
