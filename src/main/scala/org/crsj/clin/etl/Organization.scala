package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{expr, udf}

object Organization {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/org.ndjson", $"id", $"name", expr("alias[0]") as "alias")
  }
}
