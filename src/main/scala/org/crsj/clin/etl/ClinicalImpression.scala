package org.crsj.clin.etl

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

object ClinicalImpression {
  def load(base: String, practitionerWithRolesAndOrg: sql.DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val clinicalImpression = DataFrameUtils.load(s"$base/ci.ndjson", $"id", $"subject", $"status", $"effective", $"extension.valueAge.value" (0) as "runtimePatientAge", $"assessor.id" as "assessor_id")
    clinicalImpression
  }
}
