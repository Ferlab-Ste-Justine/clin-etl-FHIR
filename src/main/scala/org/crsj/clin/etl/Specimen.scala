package org.crsj.clin.etl

import org.apache.spark.sql.functions.{col, expr, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Specimen {
  def load(base: String)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._
    val rawSpecimen = DataFrameUtils.load(s"$base/sp.ndjson", $"id", $"subject", $"status", $"request.id" as "request", expr("container.identifier[0].value") as "container", $"type", expr("parent[0].id") as "parent")
    val sample = rawSpecimen.filter(col("parent").isNotNull).as("sample")
    val specimen = rawSpecimen.filter(col("parent").isNull)
    val explodedSample = sample
      .join(
          specimen.as("specimen"), 
          $"sample.parent" === $"specimen.id", 
          "left"
      ).select(
        $"sample.id",
        $"sample.subject",
        $"sample.status",
        $"sample.request",
        $"sample.container",
        $"sample.type",
        struct(
            $"specimen.id", 
            $"specimen.subject", 
            $"specimen.status",
            $"specimen.request",
            $"specimen.container",
            $"specimen.type",
        ).as("parent")
      )
    (specimen, explodedSample)
  }
}
