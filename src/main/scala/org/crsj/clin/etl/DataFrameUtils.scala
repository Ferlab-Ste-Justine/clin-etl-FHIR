package org.crsj.clin.etl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.functions._

object DataFrameUtils {

  def load(file: String, cols: Column*)(implicit spark: SparkSession): DataFrame = {
    val df = spark.read.json(file)
    if (cols.nonEmpty)
      df.select(cols: _*).persist()
    else
      df.persist()
  }


  val identifier: UserDefinedFunction = udf((data: Seq[Row]) => {
    if (data == null) None
    else {
      val d = data.collect {
        case r@Row(Row(Seq(coding: Row, _*), _*), _*) => coding.getAs[String]("code") -> r.getAs[String]("value")
      }.toMap
      if (d.nonEmpty) Some(d) else None
    }
  })

  val link: UserDefinedFunction = udf((data: Seq[Row]) => {
    if (data == null) None
    else {
      val d = data.collect {
        case r@Row(Row(Seq(coding: Row, _*), _*), _*) => coding.getAs[String]("id") -> r.getAs[String]("valueCode")
      }.toMap
      if (d.nonEmpty) Some(d) else None
    }
  })



  def joinAggregateList(df1: DataFrame, df2: DataFrame, condition: Column, columnName: String): DataFrame = {
    df1.joinWith(df2, condition, "left")
      .withColumnRenamed("_1", "r1")
      .withColumnRenamed("_2", "r2")
      .groupBy("r1.id")
      .agg(first("r1") as "r1", collect_list("r2") as columnName)
      .select("r1.*", columnName)
  }

  def joinAggregateFirst(df1: DataFrame, df2: DataFrame, condition: Column, columnName: String): DataFrame = {
    df1.joinWith(df2, condition, "left")
      .withColumnRenamed("_1", "r1")
      .withColumnRenamed("_2", "r2")
      .groupBy("r1.id")
      .agg(first("r1") as "r1", first("r2") as columnName)
      .select("r1.*", columnName)
  }

  private def dropSubColumn(col: Column, colType: DataType, fullColName: String, dropColName: String): Option[Column] = {
    if (fullColName.equals(dropColName)) {
      None
    } else if (dropColName.startsWith(s"$fullColName.")) {
      colType match {
        case colType: StructType =>
          Some(struct(
            colType.fields
              .flatMap(f =>
                dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                  case Some(x) => Some(x.alias(f.name))
                  case None => None
                })
              : _*))
        case colType: ArrayType =>
          colType.elementType match {
            case innerType: StructType =>
              Some(struct(innerType.fields
                .flatMap(f =>
                  dropSubColumn(col.getField(f.name), f.dataType, s"$fullColName.${f.name}", dropColName) match {
                    case Some(x) => Some(x.alias(f.name))
                    case None => None
                  })
                : _*))
          }

        case _ => Some(col)
      }
    } else {
      Some(col)
    }
  }

  def dropColumn(df: DataFrame, colName: String): DataFrame = {
    df.schema.fields
      .flatMap(f => {
        if (colName.startsWith(s"${f.name}.")) {
          dropSubColumn(col(f.name), f.dataType, f.name, colName) match {
            case Some(x) => Some((f.name, x))
            case None => None
          }
        } else {
          None
        }
      })
      .foldLeft(df.drop(colName)) {
        case (d, (c, column)) => d.withColumn(c, column)
      }
  }

  //  /**
  //    * Extended version of DataFrame that allows to operate on nested fields
  //    */
  //  implicit class ExtendedDataFrame(df: DataFrame) extends Serializable {
  //    /**
  //      * Drops nested field from DataFrame
  //      *
  //      * @param colName Dot-separated nested field name
  //      */
  //    def dropNestedColumn(colName: String): DataFrame = {
  //      DataFrameUtils.dropColumn(df, colName)
  //    }
  //  }

}

