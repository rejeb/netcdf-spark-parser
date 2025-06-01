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
package io.github.rejeb.netcdf.spark.reader

import io.github.rejeb.netcdf.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class NetCdfScanBuilder(schema: StructType,
                             options: CaseInsensitiveStringMap)
  extends SupportsPushDownRequiredColumns  {
  private val ncOptions: NetcdfDatasourceOptions = buildOptions()
  private var requiredSchema = schema
  lazy val sparkSession = SparkSession.active

  private def buildOptions() = {

    NetcdfDatasourceOptions(
      options.get(DatasourceOptions.PATH),
      options.getLong(DatasourceOptions.PARTITION_SIZE, -1),
      SparkUtils.coreCount(sparkSession.sparkContext.getConf),
      options.getOrDefault(DatasourceOptions.DIMS_TO_IGNORE, "").split(",").map(_.trim).toList
    )
  }

  override def build(): Scan = NetcdfScan(schema.fieldNames,requiredSchema, ncOptions)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema.fields.nonEmpty) {
      this.requiredSchema = requiredSchema
    }
  }
}
