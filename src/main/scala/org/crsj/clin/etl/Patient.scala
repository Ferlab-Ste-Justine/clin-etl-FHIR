package org.crsj.clin.etl

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{expr, udf}

import scala.collection.mutable

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
  private def linkGetter: UserDefinedFunction = {
    udf((data: Seq[Row]) => {
      if(data == null) None
      else {
        Some{
          data.map( row => {
            Array[String](row(1).asInstanceOf[Row](0).asInstanceOf[String], row(0).asInstanceOf[mutable.WrappedArray[String]](1))
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
      $"id", $"active", $"gender", $"birthDate", $"name",
      $"generalPractitioner", $"managingOrganization",
      DataFrameUtils.identifier($"identifier") as "identifier2",
      linkGetter(expr("link")) as "link2",
//      DataFrameUtils.link($"link") as "link2",
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
