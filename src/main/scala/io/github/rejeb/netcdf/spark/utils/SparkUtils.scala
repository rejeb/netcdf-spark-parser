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

import scala.util.matching.Regex

object SparkUtils {

  def coreCount(sparkConf: SparkConf): Int = {
    val masterPattern: Regex = """^local(?:\[(\d+)\]|\[(\*)\])?$""".r
    val master = sparkConf.get("spark.master", "")
    master match {
      case masterPattern(nummCores, _) if nummCores != null => nummCores.toInt
      case masterPattern(_, allCores) if (allCores != null) => availableProcessors
      case _ => sparkConf.getInt("spark.executor.cores", 1)
    }
  }

  def availableProcessors: Int = Runtime.getRuntime.availableProcessors
}
