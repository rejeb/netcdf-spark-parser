/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.rejeb.netcdf.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import java.nio.file.Paths

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
    val spark = SparkSession.builder().master("local[*]").appName("NetCdf Reader").getOrCreate()

    val expected = spark.read.parquet(expectedPath).collect()
    val result = spark
      .read
      .format("netcdf")
      .schema(schema)
      .option("path", inputPath)
      .load()
      .collect()

    result shouldBe expected
  }
}
