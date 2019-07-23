package org.crsj.clin.etl

import org.apache.spark.sql.functions.{explode, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Group {
  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val group = DataFrameUtils.load(s"$base/group.ndjson", $"id" as "group_id", expr( "member.entity.id") as "patient_id")
    val explodedGroup = group.select($"group_id", explode($"patient_id") as "patient_id")
    val study = DataFrameUtils.load(s"$base/study.ndjson", $"id", $"id" as "study_id", $"title", expr("enrollment.id") as "enrollment_id")
    val explodedStudy = study.select($"id", $"id" as "study_id", $"title", explode($"enrollment_id") as "enrollment_id")

    val explodedStudyWithExplodedGroup = explodedStudy.select( $"study_id", $"title", $"enrollment_id")
      .join(explodedGroup.select($"group_id", $"patient_id"), $"enrollment_id" === $"group_id")
      .select($"study_id" as "id", $"title", $"patient_id" as "patient")

    explodedStudyWithExplodedGroup
  }
}
