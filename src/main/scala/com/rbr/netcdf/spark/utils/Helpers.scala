package com.rbr.netcdf.spark.utils

object Helpers {
  def crossJoin[T](in: List[List[T]]): List[List[T]] =
    in.foldRight(List(List.empty[T])) {
      for {left <- _; right <- _} yield left :: right
    }
}
