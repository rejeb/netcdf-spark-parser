package com.rbr.netcdf.spark.utils

import com.rbr.netcdf.spark.utils.NetCdfFileReader.openFile
import org.apache.spark.internal.Logging
import ucar.nc2.{NetcdfFile, NetcdfFiles}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.util.{Failure, Success, Try}

class NetCdfFileReader(path: String, poolSize: Int) extends Serializable with AutoCloseable with Logging {
  private val ncFilePool: ConcurrentLinkedQueue[NetcdfFile] = {
    val queue = new ConcurrentLinkedQueue[NetcdfFile]
    (0 until poolSize).foreach(_ => queue.add(openFile(path)))
    queue
  }

  def aquire(): NetcdfFile = {
    if (ncFilePool.isEmpty) {
      ncFilePool.add(openFile(path))
    }
    ncFilePool.poll()
  }

  def release(file: NetcdfFile): Unit = {
    file.getVariables.forEach(v => {
      if (v.hasCachedData) {
        v.createNewCache()
      }
    })
    ncFilePool.add(file)
  }

  override def close(): Unit = {
    logInfo("Closing all files...")
    while (!ncFilePool.isEmpty) {
      try {
        ncFilePool.poll().close()
      } catch {
        case _: Throwable => logDebug("Error closing file.")
      }
    }
  }
}

object NetCdfFileReader {
  private[spark] def openFile(fileName: String): NetcdfFile = {
    Try(NetcdfFiles.open(fileName)) match {
      case Success(file: NetcdfFile) => file
      case Failure(exception) => throw exception
    }
  }
}