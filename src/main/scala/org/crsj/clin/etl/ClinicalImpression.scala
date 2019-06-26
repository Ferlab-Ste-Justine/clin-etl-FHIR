package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object ClinicalImpression {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/ci.ndjson", $"id", $"subject", $"status", $"effective",
      $"subject", $"extension.valueAge.value" (0) as "runtimePatientAge")
  }
}
