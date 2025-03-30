package com.rbr.netcdf.spark.reader

import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class NetCdfScanBuilder(schema: StructType, options: CaseInsensitiveStringMap) extends SupportsPushDownRequiredColumns {
  private val ncOptions: NetcdfDatasourceOptions = buildOptions()
  private var requiredSchema = schema

  private def buildOptions() = {
    NetcdfDatasourceOptions(
      options.get("path"),
      options.getLong("partitionSize", -1),
      options.getInt("poolSize", 5)
    )
  }

  override def build(): Scan = new NetcdfScan(requiredSchema, ncOptions)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (requiredSchema.fields.nonEmpty) {
      this.requiredSchema = requiredSchema
    }
  }
}
