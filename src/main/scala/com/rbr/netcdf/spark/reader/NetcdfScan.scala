package com.rbr.netcdf.spark.reader

import com.rbr.netcdf.spark.utils.Helpers.crossJoin
import com.rbr.netcdf.spark.utils.NetCdfFileReader
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import ucar.nc2.Dimension

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

class NetcdfScan(schema: StructType, options: NetcdfDatasourceOptions) extends Scan with Batch with Logging {
  private val maxNumPartitions: Int = 2000
  private val minRowsPerPartition: Int = 20000
  private lazy val variablesList: Array[String] = schema.fieldNames

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    logInfo("Start plan partitions...")
    val ncFile = NetCdfFileReader.openFile(options.path)
    val wantedVariables = ncFile.getVariables.asScala.filter(v => variablesList.contains(v.getShortName))
    val dimensions: List[Dimension] = ncFile
      .getRootGroup
      .getDimensions.asScala
      .filter(d =>
        wantedVariables.count(v =>
          v.getDimensions.asScala.exists(_.getShortName == d.getShortName)) > 1).toList

    if (dimensions.isEmpty) {
      throw new SparkException("Variables should share at least one common dimension.")
    }
    val partitionSize = computePartitionSize(dimensions)
    val partitions: Seq[NetcdfInputSplit] = sliceDimensions(dimensions, partitionSize).map(NetcdfInputSplit(_))

    logInfo(s"Number of partition is : ${partitions.size}")

    if (log.isDebugEnabled()) {
      partitions.zipWithIndex.foreach(p => logInfo(s"Partition ${p._2}: ${p._1.dimensions.toString()}"))
    }

    partitions.toArray

  }

  override def createReaderFactory(): PartitionReaderFactory = new NetcdfPartitionReaderFactory(schema, options)

  private def computePartitionSize(dimensions: List[Dimension]): Long = {
    if (options.partitionSize != -1) {
      options.partitionSize
    } else {
      val rowCount = dimensions.map(_.getLength.toLong).product
      if (rowCount / minRowsPerPartition < maxNumPartitions) {
        minRowsPerPartition
      } else {
        (rowCount / maxNumPartitions)
      }
    }
  }

  private def sliceDimensions(dimensions: List[Dimension], partitionSize: Long): List[List[DimensionPartition]] = {
    val maxElemPerDim = computeMaxElemPerDim(dimensions.map(d => (d.getShortName, d.getLength.toLong)).toMap, partitionSize)

    crossJoin(dimensions.map(d => {
      buildNetcdfDim(d.getShortName, d.getLength, maxElemPerDim(d.getShortName).toInt)
    }))
  }

  @tailrec
  private def computeMaxElemPerDim(dimensions: Map[String, Long], partitionSize: Long): Map[String, Long] = {
    if (dimensions.values.product <= partitionSize) {
      dimensions
    } else {
      computeMaxElemPerDim(dimensions.map(d => (d._1 -> (if (d._2 <= 2) d._2 else (d._2 - 1)))), partitionSize)
    }
  }

  private def buildNetcdfDim(name: String, length: Int, maxElem: Int): List[DimensionPartition] = {
    buildListIndexes(length, maxElem, List(length)).sliding(2, 1)
      .map { case List(begin, end) =>
        DimensionPartition(name, begin, end)
      }.toList
  }

  @tailrec
  private def buildListIndexes(length: Int, maxElem: Int, acc: List[Int]): List[Int] = {
    val head = acc.head
    val next = head - maxElem
    if (next > 0) {
      buildListIndexes(length, maxElem, next :: acc)
    } else {
      (0) :: acc
    }
  }
}
