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
  case class Family(relationship: String, id: String)

  private def linkGetter: UserDefinedFunction = {
    udf((data: Seq[Row]) => {
      if(data == null) None
      else {
        Some{
          data.map( row => {
            //TODO ¯\_(ツ)_/¯ row.toSeq fails, row(1) fails, row.getAs fails, but it prints alright

            val myDirtyHack = row.toString()
              .replaceAll("WrappedArray", "")
              .replaceAll("\\(", "")
              .replaceAll("\\)", "")
              .replaceAll("\\[", "")
              .replaceAll("\\]", "")
              .split(",")

            Family(myDirtyHack(1), myDirtyHack(2))

            //TODO End. Please don't judge us too hard for this when you refactor it :slightly_smiling_face:
          })
        }
      }
    })
  }

  private val family: UserDefinedFunction = patientExtension[String]("familyId", "valueId")
  private val ethnicity: UserDefinedFunction = patientExtension[String]("ethnicity", "valueCode")
  private val familyComposition: UserDefinedFunction = patientExtension[String]("familyComposition", "valueCode")
  private val isProband: UserDefinedFunction = patientExtension[Boolean]("isProband", "valueBoolean")
  private val status: UserDefinedFunction = patientExtension[Boolean]("status", "valueBoolean")

  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val patients = DataFrameUtils.load(s"$base/pt.ndjson",
      $"id", $"active", $"gender", $"birthDate", $"name",
      $"generalPractitioner", $"managingOrganization",
      DataFrameUtils.identifier($"identifier") as "identifier2",
      linkGetter(expr("link")) as "link2",
      family(expr("extension[0].extension")) as "familyId",
      ethnicity(expr("extension[0].extension")) as "ethnicity",
      familyComposition(expr("extension[0].extension")) as "familyComposition",
      isProband(expr("extension[0].extension")) as "isProband",
      status(expr("extension[0].extension")) as "status"
    )
      .withColumnRenamed("identifier2", "identifier")
      .withColumnRenamed("link2", "link")
    patients
  }
}
