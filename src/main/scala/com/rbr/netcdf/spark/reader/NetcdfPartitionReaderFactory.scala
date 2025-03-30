package com.rbr.netcdf.spark.reader

import com.rbr.netcdf.spark.utils.NetCdfFileReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class NetcdfPartitionReaderFactory(schema: StructType, options: NetcdfDatasourceOptions) extends PartitionReaderFactory {
  private lazy val reader = new NetCdfFileReader(options.path, options.poolSize)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new NetcdfPartitionReader(reader, schema, partition.asInstanceOf[NetcdfInputSplit])
  }
}
