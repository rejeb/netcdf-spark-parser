import Versions.*
import sbt.*

object Dependencies {
  val cdmS3 = "edu.ucar" % "cdm-s3" % netcdfVersion
  val netCdfJavaPlatform = "edu.ucar" % "netcdf-java-platform" % netcdfVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % Test
  val mockitoScala = "org.mockito" %% "mockito-scala-scalatest" % mockitoVersion % Test
  val mockitoInline = "org.mockito" % "mockito-inline" % mockitoInlineVersion % Test
}
