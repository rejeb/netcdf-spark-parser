package com.rbr.netcdf.spark

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class DefaultSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = throw new UnsupportedOperationException("Schema is required.")

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new NetcdfTable(schema, partitioning, properties.asScala.toMap)
  }

  override def shortName(): String = "netcdf"

  override def supportsExternalMetadata(): Boolean = true
}
