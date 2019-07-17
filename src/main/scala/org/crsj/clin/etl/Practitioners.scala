package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

object Practitioners {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val practitioners = DataFrameUtils.load(s"$base/pr.ndjson", $"id", $"name", DataFrameUtils.identifier($"identifier") as "identifier2").withColumnRenamed("identifier2", "identifier")
    val practitionerRoles = DataFrameUtils.load(s"$base/prr.ndjson", $"id", $"practitioner")

    val practitionerWithRoles = practitionerRoles.joinWith(practitioners, practitioners("id") === practitionerRoles("practitioner.id"))
      .withColumnRenamed("_1", "practionerRole")
      .withColumnRenamed("_2", "practitioner")
      .select($"practionerRole.id" as "role_id", $"practitioner.*")
    practitionerWithRoles

  }
}
