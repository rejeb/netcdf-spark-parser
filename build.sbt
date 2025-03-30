ThisBuild / version := "0.1.0-SNAPSHOT"
resolvers +=
  "Unidata All" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "netcdf-spark-parser"
  )
Test / parallelExecution := false
libraryDependencies += "edu.ucar" % "cdm-s3" % "5.7.0" % Provided
libraryDependencies += "edu.ucar" % "netcdf-java-platform" % "5.7.0" % Provided
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5" % Provided
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test