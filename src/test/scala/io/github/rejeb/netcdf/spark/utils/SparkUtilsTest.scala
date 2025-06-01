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
package io.github.rejeb.netcdf.spark.utils

import org.apache.spark.SparkConf
import org.mockito.MockitoSugar
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkUtilsTest extends AnyFunSuite with Matchers {

  test("Should return num cores=6 for local[6]") {
    val sparkConf = new SparkConf().set("spark.master", "local[6]")
    val expected: Int = 6
    val result: Int = SparkUtils.coreCount(sparkConf)

    result shouldBe expected
  }

  test("Should return num cores=availableProcessors for local[*] ") {
    val sparkUtilsMock = mock[SparkUtils.type]

    MockitoSugar.mock[Runtime]
    val sparkConf = new SparkConf().set("spark.master", "local[*]")
    val expected: Int = 99
    when(sparkUtilsMock.availableProcessors).thenReturn(99)
    when(sparkUtilsMock.coreCount(sparkConf)).thenCallRealMethod()
    val result: Int = sparkUtilsMock.coreCount(sparkConf)

    result shouldBe expected
  }

  test("Should return num cores=1 for master = local ") {
    val sparkConf = new SparkConf().set("spark.master", "local")
    val expected: Int = 1
    val result: Int = SparkUtils.coreCount(sparkConf)
    result shouldBe expected
  }

  test("Should return num cores=4 for master != local and spark.executor.cores=4 ") {
    val sparkConf = new SparkConf().set("spark.master", "yarn").set("spark.executor.cores", "4")
    val expected: Int = 4
    val result: Int = SparkUtils.coreCount(sparkConf)
    result shouldBe expected
  }

}
