package org.crsj.clin.etl

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

  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/obs.ndjson", $"id", $"status", $"code", $"subject", $"effective",
      phenotypes($"value.CodeableConcept.coding") as "phenotype", $"note",
      expr("interpretation[0].coding[0].code") as "display"
    )


  }

}
