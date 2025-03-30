package com.rbr.netcdf.spark.reader

case class NetcdfDatasourceOptions(path: String, partitionSize: Long, poolSize: Int)
