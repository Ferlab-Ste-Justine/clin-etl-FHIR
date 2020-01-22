package org.crsj.clin.etl

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{expr, udf}



object ServiceRequest {



  def load(base: String, practitionerWithRolesAndOrg: sql.DataFrame, clinicalImpressions: sql.DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val serviceRequest = DataFrameUtils.load(s"$base/sr.ndjson",$"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen",
      //refGetter(expr("extension.valueReference.reference")) as "ci_ref")
    expr("extension[0].valueReference.reference").substr(20,7) as "ci_ref", $"requester.id" as "requester_role_id")

    val serviceRequestWithClinicalImpression = serviceRequest
      .select($"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen", $"ci_ref", $"requester_role_id")
      .join(clinicalImpressions.select($"status" as "ci_status", $"id" as "ci_id", $"assessor_id"), $"ci_ref" === $"ci_id")

    val serviceRequestWithClinicalImpressionAndRequester = serviceRequestWithClinicalImpression
      .select($"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"specimen", $"ci_ref", $"requester_role_id",$"ci_status")
      .join(practitionerWithRolesAndOrg
      .select($"role_id", $"name" as "requester_name", $"org_name" as "requester_org_name",
        $"org_alias" as "requester_org_alias",  $"org_id" as "requester_org_id", $"id"),
        $"requester_role_id" === $"role_id")


    serviceRequestWithClinicalImpressionAndRequester
  }
}
