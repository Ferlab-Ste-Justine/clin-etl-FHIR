package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}

object Patient {

  case class Relationship (id: String, relation: String)

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

  private val linkToPatient = udf((data: Seq[Row]) => {
    if (data == null) None
    else Some {
      data.map { r => Relationship(r.getAs("other.id"), r.getAs("extension.valueCode"))
      }
    }
  })

  private def linkGetter: UserDefinedFunction = {
    udf((data: Seq[Row]) => {
      if(data == null) None
      else {
        Some{
          print("DATA: ")
          println(data)
          data.map( row => {
            print("ROW: "); println(row)
            Array[String](row.getAs[String]("other.id"), row.getAs[String]("extension.valueCode"))
          })
        }
      }
    })
  }

  private val family: UserDefinedFunction = patientExtension[String]("familyId", "valueId")
  private val ethnicity: UserDefinedFunction = patientExtension[String]("ethnicity", "valueCode")
  private val familyComposition: UserDefinedFunction = patientExtension[String]("familyComposition", "valueCode")
  private val isProband: UserDefinedFunction = patientExtension[Boolean]("isProband", "valueBoolean")

  def load(base: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val patients = DataFrameUtils.load(s"$base/pt.ndjson",
      $"id", $"active", $"gender", $"birthDate",
      $"generalPractitioner", $"managingOrganization",
      DataFrameUtils.identifier($"identifier") as "identifier2",
      //linkGetter(expr("link")) as "link2",
      linkToPatient($"link") as "link2",
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
