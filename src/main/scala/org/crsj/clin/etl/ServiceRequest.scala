package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}
import org.crsj.clin.etl.Patient.patientExtension



object ServiceRequest {

  import scala.reflect.runtime.universe.TypeTag




  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val sr = DataFrameUtils.load(s"$base/sr.ndjson",
      $"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen",
      //refGetter(expr("extension.valueReference.reference")) as "ci_ref")
    expr("extension[0].valueReference.reference").substr(20,7) as "ci_ref")

    val clinicalImpressions = DataFrameUtils.load(s"$base/ci.ndjson", $"id" as "ci_id", $"subject", $"status", $"effective")

    val serviceRequestWithClinicalImpression = sr.select($"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen", $"ci_ref")
      .join(clinicalImpressions.select("*"), $"ci_ref" === $"ci_id")

  serviceRequestWithClinicalImpression
  }
}
