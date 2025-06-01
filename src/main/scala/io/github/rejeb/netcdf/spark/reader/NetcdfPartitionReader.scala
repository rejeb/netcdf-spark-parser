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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import ucar.ma2
import ucar.ma2.DataType
import ucar.nc2.{Dimension, NetcdfFile, Variable}

import java.util
import scala.jdk.CollectionConverters._

class NetcdfPartitionReader(reader: NetCdfFileReader,
                            schema: StructType,
                            inputPartition: NetcdfInputSplit) extends PartitionReader[InternalRow] {

  private lazy val iterator: Iterator[InternalRow] = buildIterator()


  override def next: Boolean = iterator.hasNext

  override def get: InternalRow = iterator.next()

  override def close(): Unit = {
  }


  private def buildIterator() = {
    val ncFilePartition = reader.aquire()
    val arrayRangesPerDim = inputPartition.dimensions.map(d => (d.name, (d.start, d.end))).toMap
    val allCellIndexPerDim = computeAllPossibleCellIndexes(inputPartition.dimensions)
    val variables = variablesToReadFrom(ncFilePartition)
    val arrayData: Map[String, ma2.Array] = variables
      .map(variable => (variable.getShortName -> loadDataAtRanges(arrayRangesPerDim, variable))).toMap
    val data: List[InternalRow] = allCellIndexPerDim.map(retrieveDataAsRow(_, variables, arrayData))
    reader.release(ncFilePartition)
    data.iterator
  }

  private def computeAllPossibleCellIndexes(dimensions: List[DimensionChunk]): List[Map[String, Int]] = {
    val intervals = dimensions
      .map(e => {
        (0 until (e.end - e.start)).map((e.name, _)).toList
      })
    crossJoin(intervals)
      .map(_.toMap)
  }

  private def variablesToReadFrom(ncFilePartition: NetcdfFile): Seq[Variable] = {
    ncFilePartition
      .getVariables
      .asScala
      .filter(v => schema.fieldNames.contains(v.getShortName))
      .toSeq
  }

  private def loadDataAtRanges(arrayRangesPerDim: Map[String, (Int, Int)], variable: Variable): ma2.Array = {
    val ranges = variable.getDimensions.asScala.map(d => {
      if (arrayRangesPerDim.contains(d.getShortName)) {
        new ucar.ma2.Range(arrayRangesPerDim(d.getShortName)._1, arrayRangesPerDim(d.getShortName)._2 - 1)
      } else {
        new ucar.ma2.Range(0, d.getLength - 1)
      }
    }).toList.asJava
    variable.read(ranges)
  }

  private def retrieveDataAsRow(cellIndexPerDim: Map[String, Int], variables: Seq[Variable],
                                arrayRangesPerDim: Map[String, ma2.Array]
                               ): InternalRow = {
    val row = variables.map(variable => {
      val value = getValue(cellIndexPerDim,
        variable.getDimensions.asScala.toList,
        arrayRangesPerDim(variable.getShortName),
        variable.getShortName)
      (variable.getShortName -> value)
    }).toMap
    InternalRow(schema.map(c => row(c.name)).toArray: _*)
  }

  private def getValue(cellIndexPerDim: Map[String, Int],
                       dimensions: List[Dimension],
                       varray: ma2.Array,
                       columnName: String): Any = {
    if (dimensions.forall(d => cellIndexPerDim.contains(d.getShortName))) {
      getSingleElem(cellIndexPerDim, dimensions, varray)
    } else {
      getArrayOrString(cellIndexPerDim, dimensions, varray, columnName)
    }
  }

  private def getSingleElem(points: Map[String, Int], dimensions: List[Dimension], varray: ma2.Array): Any = {
    val ranges: util.List[ma2.Range] = dimensions.map(d => {
      if (points.contains(d.getShortName)) {
        new ucar.ma2.Range(points(d.getShortName), points(d.getShortName))
      } else {
        throw new RuntimeException(s"Dimension ${d.getShortName} is missing.")
      }
    }).asJava
    val arrayAtSection = varray.sectionNoReduce(ranges)
    parse(arrayAtSection)
  }

  private def getArrayOrString(cellIndexPerDim: Map[String, Int], dimensions: List[Dimension], varray: ma2.Array, columnName: String): Any = {
    val allPossibleIndexesPerDim = crossJoin(dimensions.map(d => {
      if (cellIndexPerDim.contains(d.getShortName)) {
        List((d.getShortName, cellIndexPerDim(d.getShortName)))
      } else {
        (0 until d.getLength).map((d.getShortName, _)).toList
      }
    })).map(_.toMap)
    val content: Seq[Any] = allPossibleIndexesPerDim.map(getSingleElem(_, dimensions, varray))
    varray.getDataType match {
      case DataType.STRING | DataType.CHAR => UTF8String.fromString(content.mkString(""))
      case _ => ArrayData.toArrayData(content)
    }
  }

  private def parse(arrayOfData: ma2.Array): Any = {
    val currentIndex = arrayOfData.getIndex.currentElement()
    arrayOfData.getDataType match {
      case DataType.INT => arrayOfData.getInt(currentIndex)
      case DataType.SHORT => arrayOfData.getInt(currentIndex)
      case DataType.DOUBLE => arrayOfData.getDouble(currentIndex)
      case DataType.FLOAT => arrayOfData.getFloat(currentIndex)
      case DataType.CHAR => UTF8String.fromString(arrayOfData.getChar(currentIndex).toString)
      case DataType.STRING => UTF8String.fromString(arrayOfData.getChar(currentIndex).toString)
      case DataType.LONG => arrayOfData.getLong(currentIndex)
      case DataType.BOOLEAN => arrayOfData.getBoolean(currentIndex)
      case _ => throw UnsupportedDataTypeException(s"Unsupported data type ${arrayOfData.getDataType}")
    }
  }

}
