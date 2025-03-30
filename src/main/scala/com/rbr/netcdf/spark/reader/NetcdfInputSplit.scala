package com.rbr.netcdf.spark.reader

import org.apache.spark.sql.connector.read.InputPartition

case class NetcdfInputSplit(dimensions: List[DimensionPartition]) extends InputPartition