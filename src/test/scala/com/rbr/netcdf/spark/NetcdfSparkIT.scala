package com.rbr.netcdf.spark

import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import java.nio.file.{Files, Paths}

class NetcdfSparkIT extends AnyFunSuite with Matchers with BeforeAndAfter with BeforeAndAfterAll {



  test("Can read NetcdfFile") {
    val inputPath = Paths.get("src/test/resources/input/madis-hydro.nc").toFile.getAbsolutePath
    val expectedPath = Paths.get("src/test/resources/expected/madis-hydro").toFile.getAbsolutePath
    val schema: StructType = StructType(Seq(
      StructField("precip12hr", FloatType),
      StructField("precip12hrDD", StringType),
      StructField("precip12hrQCA", IntegerType),
      StructField("precip12hrQCR", IntegerType),
      StructField("precip12hrQCD", ArrayType(FloatType)),
      StructField("precip12hrICA", IntegerType),
      StructField("precip12hrICR", IntegerType),
      StructField("rawMessage", StringType)
    ))
    val spark = SparkSession.builder().master("local[8]").appName("NetCdf Reader").getOrCreate()

    val expected = spark.read.parquet(expectedPath).collect()
    val result = spark
      .read
      .format("com.rbr.netcdf.spark")
      .schema(schema)
      .option("path", inputPath)
      .load()
      .collect()

    result shouldBe expected
  }
}
