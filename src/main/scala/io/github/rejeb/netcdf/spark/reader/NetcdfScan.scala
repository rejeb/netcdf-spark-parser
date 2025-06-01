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

import io.github.rejeb.netcdf.spark.utils.Helpers.crossJoin
import io.github.rejeb.netcdf.spark.utils.NetCdfFileReader
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.{ArrayType, StructType}
import ucar.nc2.{Dimension, Variable}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

case class NetcdfScan(variablesList: Array[String], schema: StructType,
                      options: NetcdfDatasourceOptions) extends Scan with Batch with Logging {
  private val maxNumPartitions: Int = 2000
  private val minRowsPerPartition: Int = 20000
  private val maxRowsPerPartition: Int = 1000000

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    logInfo("Start plan partitions...")
    val ncFile = NetCdfFileReader.openFile(options.path)
    val wantedVariables: Seq[Variable] = ncFile.getVariables.asScala.filter(v => variablesList.contains(v.getShortName)).toSeq
    val minCommonDimsCount = if (wantedVariables.size > 1) 2 else 1
    val dimensions: List[Dimension] = ncFile
      .getRootGroup
      .getDimensions
      .asScala
      .filter(d => !options.dimensionsToIgnore.contains(d.getShortName) &&
        wantedVariables.count(v =>
          v.getDimensions.asScala.exists(_.getShortName == d.getShortName)) >= minCommonDimsCount).toList

    checkSchema(wantedVariables, dimensions)

    if (dimensions.isEmpty) {
      throw new SparkException("Variables should share at least one common dimension.")
    }
    val partitionSize = computePartitionSize(dimensions)
    val sizePerDim = dimensions.map(d => (d.getShortName -> d.getLength.toLong)).toMap
    val partitions: Seq[NetcdfInputSplit] = buildPartitionMatrix(sizePerDim, partitionSize).map(NetcdfInputSplit(_))

    logInfo(s"Number of partition is : ${partitions.size}")

    if (log.isDebugEnabled()) {
      partitions.zipWithIndex.foreach(p => logInfo(s"Partition ${p._2}: ${p._1.dimensions.toString()}"))
    }

    partitions.toArray

  }

  override def createReaderFactory(): PartitionReaderFactory = NetcdfPartitionReaderFactory(schema, options)

  private def computePartitionSize(dimensions: List[Dimension]): Long = {
    if (options.partitionSize != -1) {
      options.partitionSize
    } else {
      val rowCount = dimensions.map(_.getLength.toLong).product
      if (rowCount / minRowsPerPartition < maxNumPartitions) {
        minRowsPerPartition
      } else if (rowCount / maxNumPartitions > maxRowsPerPartition) {
        maxRowsPerPartition
      } else {
        (rowCount / maxNumPartitions)
      }
    }
  }

  private def buildPartitionMatrix(sizePerDim: Map[String, Long], partitionSize: Long): List[List[DimensionChunk]] = {
    val maxElemPerDim = dimensionSizePerPartition(sizePerDim, partitionSize)
    crossJoin(sizePerDim.map(d => {
      buildChunk(d._1, d._2.toInt, maxElemPerDim(d._1).toInt)
    }).toList)
  }

  @tailrec
  private def dimensionSizePerPartition(dimensions: Map[String, Long], partitionSize: Long): Map[String, Long] = {
    if (dimensions.values.product <= partitionSize) {
      dimensions
    } else {
      dimensionSizePerPartition(dimensions.map(d => (d._1 -> (if (d._2 <= 2) d._2 else (d._2 - 1)))), partitionSize)
    }
  }

  private def buildChunk(dimensionName: String, dimensionLength: Int, step: Int): List[DimensionChunk] = {
    generateOffsets(dimensionLength, step, List(dimensionLength)).sliding(2, 1)
      .map { case List(begin, end) =>
        DimensionChunk(dimensionName, begin, end)
      }.toList
  }

  @tailrec
  private def generateOffsets(from: Int, step: Int, acc: List[Int]): List[Int] = {
    val next = from - step
    if (next <= 0) 0 :: acc
    else generateOffsets(next, step, next :: acc)
  }

  private def checkSchema(variables: Seq[Variable], commonDims: Seq[Dimension]): Unit = {
    this.schema.fields.foreach(f => {
      f.dataType match {
        case ArrayType(_, _) => {
          val variable = variables.find(v => v.getShortName == f.name).get
          val dimensions = variable.getDimensions.asScala.toList

          if (dimensions.forall(d => commonDims.exists(d.getShortName == _.getShortName))) {
            throw new SparkException(s"Invalid schema for field ${f.name}. " +
              s"Field cannot be mapped to ArrayType. " +
              s"Please change field data type or use 'dimensions.to.ignore' to ignore at least one of field dimensions.")
          }
        }
        case _ =>
      }
    })
  }
}
