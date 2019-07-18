package org.crsj.clin.etl

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{expr, udf}



object ServiceRequest {



  def load(base: String, practitionerWithRoles: sql.DataFrame, clinicalImpressions: sql.DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val sr = DataFrameUtils.load(s"$base/sr.ndjson",
      $"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen",
      //refGetter(expr("extension.valueReference.reference")) as "ci_ref")
    expr("extension[0].valueReference.reference").substr(20,7) as "ci_ref", $"requester.id" as "requester_id")

    val serviceRequestWithClinicalImpression = sr.select($"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen", $"ci_ref", $"requester_id")
      .join(clinicalImpressions.select($"status" as "ci_status", $"id" as "ci_id"), $"ci_ref" === $"ci_id")

    val serviceRequestWithClinicalImpressionAndRequester = serviceRequestWithClinicalImpression.select($"id", $"status", $"intent", $"authoredOn", $"code", $"subject",
      $"specimen", $"ci_ref", $"requester_id",$"ci_status").join(practitionerWithRoles.select($"role_id", $"name"), $"requester_id" === $"role_id")


    serviceRequestWithClinicalImpressionAndRequester
//    serviceRequestWithClinicalImpression
  }
}
