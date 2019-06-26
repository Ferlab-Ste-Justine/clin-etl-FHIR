package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}
import org.crsj.clin.etl.Patient.patientExtension



object ServiceRequest {

  import scala.reflect.runtime.universe.TypeTag

  private def serviceRequestExtension[T: TypeTag](url: String, columnName: String): UserDefinedFunction = {
    udf((data: Seq[Row]) => {
      if (data == null) None
      else {
        val d: Option[T] = data.collectFirst {
          case r@Row(u: String, _*) if u == url => r.getAs[T](columnName)
        }
        d
      }
    })
  }


  private val clinicalImpression: UserDefinedFunction = serviceRequestExtension[String]("familyId", "valueId")

  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    DataFrameUtils.load(s"$base/sr.ndjson",
      $"id", $"status", $"intent", $"authoredOn", $"code", $"subject", $"speciment",
      $"extension.valueReference.reference" ("") as "ci_ref")
  }
}
