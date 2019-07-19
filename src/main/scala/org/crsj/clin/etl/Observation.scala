package org.crsj.clin.etl

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{expr, udf}

object Observation {

  case class Phenotype(code: String, display: String)

  private val phenotypes = udf((data: Seq[Row]) => {
    if (data == null) None
    else Some {
      data.map { r => Phenotype(r.getAs("code"), r.getAs("display"))
      }
    }
  })

  def load(base: String, practitionerWithRolesAndOrg: sql.DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val observations = DataFrameUtils.load(s"$base/obs.ndjson", $"id", $"status", $"code", $"subject", $"effective",
      phenotypes($"value.CodeableConcept.coding") as "phenotype", $"note",
      expr("interpretation[0].coding[0].code") as "observed", $"id" as "obs_id", expr("performer[0].id") as "performer"
    )

//    val observationsWithPerformer = observations
//      .select($"id", $"status", $"code", $"subject", $"effective", $"phenotype", $"note", $"observed", $"obs_id", $"performer")
//      .join(practitionerWithRolesAndOrg
//        .select($"role_id" as "performer_role_id", $"name" as "performer_name", $"org_name" as "performer_org_name"), $"performer" === $"performer_role_id")
    //observationsWithPerformer
    observations

  }

}
