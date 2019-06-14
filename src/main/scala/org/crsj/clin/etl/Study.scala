package org.crsj.clin.etl

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

object Study {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val group = DataFrameUtils.load(s"$base/group.ndjson")
    val study = DataFrameUtils.load(s"$base/study.ndjson")


    val studyWithPatients = study.select($"id" as "study_id", $"title", explode($"enrollment") as "enrollment")
      .join(group.select($"id" as "group_id", $"member"), $"enrollment.id" === $"group_id")
      .select($"study_id" as "id", $"title", explode($"member") as "patient")

    studyWithPatients
  }
}
