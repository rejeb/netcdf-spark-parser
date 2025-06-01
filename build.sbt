
resolvers +=
  "Unidata All" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"

javacOptions ++= Seq("-source", "11", "-target", "11")
ThisBuild / organization := "io.github.rejeb"
ThisBuild / version := "0.0.2-SNAPSHOT"
ThisBuild / scalaVersion := Versions.scalaVersion
ThisBuild / crossScalaVersions := Versions.supportedScalaVersions
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/rejeb/netcdf-spark-parser"),
    "scm:git@github.com:rejeb/netcdf-spark-parser"
  )
)
ThisBuild / developers := List(
  Developer(
    id = "rbenrejeb",
    name = "BEN REJEB",
    email = "benrejebrejeb@gmail.com",
    url = url("https://github.com/rejeb")
  )
)

ThisBuild / description := "Scala/Spark Netcdf connector for reading Netcdf files"
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage := Some(url("https://github.com/rejeb/netcdf-spark-parser"))

ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishMavenStyle := true
ThisBuild / sbtPluginPublishLegacyMavenStyle := false

ThisBuild / publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at "https://central.sonatype.com/repository/maven-snapshots/" )
  else
    localStaging.value
}


lazy val root = (project in file("."))
  .settings(
    name := "netcdf-spark-parser"
  )
Test / parallelExecution := false
libraryDependencies ++= Seq(
  Dependencies.netCdfJavaPlatform,
  Dependencies.cdmS3,
  Dependencies.sparkSql,
  Dependencies.scalaTest,
  Dependencies.mockitoScala,
  Dependencies.mockitoInline)