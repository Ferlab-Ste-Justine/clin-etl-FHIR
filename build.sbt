name := "clin-etl"

version := "0.1"

scalaVersion := "2.11.12"

val spark_version = "2.4.3"


libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version % Provided
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.7.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test

assemblyJarName in assembly := "clin-etl.jar"
crossPaths := false