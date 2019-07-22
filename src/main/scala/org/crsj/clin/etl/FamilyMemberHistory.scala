package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object FamilyMemberHistory {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/fmh.ndjson", $"id" as "fmh_id", $"status", $"patient", $"relationship", $"date", $"note")
  }
}
