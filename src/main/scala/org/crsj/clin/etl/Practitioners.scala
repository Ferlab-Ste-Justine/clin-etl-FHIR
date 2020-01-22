package org.crsj.clin.etl

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

object Practitioners {
  def load(base: String, organization: sql.DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val practitioners = DataFrameUtils.load(s"$base/pr.ndjson", $"id", $"name", DataFrameUtils.identifier($"identifier") as "identifier2").withColumnRenamed("identifier2", "identifier")
    val practitionerRoles = DataFrameUtils.load(s"$base/prr.ndjson", $"id", $"practitioner", $"organization.id" as "org_id")

    val practitionerWithRoles = practitionerRoles.joinWith(practitioners, practitioners("id") === practitionerRoles("practitioner.id"))
      .withColumnRenamed("_1", "practionerRole")
      .withColumnRenamed("_2", "practitioner")
      .select($"practionerRole.id" as "role_id", $"practionerRole.org_id" as "role_org_id", $"practitioner.*")

    val practitionerWithRolesAndOrg = practitionerWithRoles.joinWith(organization, organization("id") === practitionerWithRoles("role_org_id"))
        .withColumnRenamed("_1", "practitionerWithRoles")
        .withColumnRenamed("_2", "organization")
        .select($"practitionerWithRoles.*", $"practitionerWithRoles.id" as "pract_id" ,$"organization.name" as "org_name", $"organization.alias" as "org_alias", $"organization.id" as "org_id")
    practitionerWithRolesAndOrg
  }
}
