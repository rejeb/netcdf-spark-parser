package com.rbr.netcdf.spark

import com.rbr.netcdf.spark.reader.NetCdfScanBuilder
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._

class NetcdfTable(schema: StructType, partitioning: Array[Transform], properties: Map[String, String]) extends SupportsRead {

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new NetCdfScanBuilder(schema, options)

  override def name(): String = ""

  override def schema(): StructType = schema

  override def columns(): Array[Column] = super.columns()

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava
}
