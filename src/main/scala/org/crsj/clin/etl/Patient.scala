package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}

object Patient {

  import scala.reflect.runtime.universe.TypeTag

  private def patientExtension[T: TypeTag](url: String, columnName: String): UserDefinedFunction = {
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

  private val family: UserDefinedFunction = patientExtension[String]("familyId", "valueId")
  private val ethnicity: UserDefinedFunction = patientExtension[String]("ethnicity", "valueCode")
  private val familyComposition: UserDefinedFunction = patientExtension[String]("familyComposition", "valueCode")
  private val familyRelation: UserDefinedFunction = patientExtension[String]("valueCode", "valueCode")
  private val isProband: UserDefinedFunction = patientExtension[Boolean]("isProband", "valueBoolean")

  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val patients = DataFrameUtils.load(s"$base/pt.ndjson",
      $"id", $"active", $"gender", $"birthDate",
      $"generalPractitioner", $"managingOrganization",
      DataFrameUtils.identifier($"identifier") as "identifier2",
      $"link.other.id" as "link2",
      $"link.extension" as "link2",
//      familyRelation(expr("link[0]")) as "relation",
      family(expr("extension[0].extension")) as "familyId",
      ethnicity(expr("extension[0].extension")) as "ethnicity",
      familyComposition(expr("extension[0].extension")) as "familyComposition",
      isProband(expr("extension[0].extension")) as "isProband"
    )
      .withColumnRenamed("identifier2", "identifier")
      .withColumnRenamed("link2", "link")
    patients
  }
}
