package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object Organization {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/org.ndjson", $"id", $"name", $"alias")
  }
}
