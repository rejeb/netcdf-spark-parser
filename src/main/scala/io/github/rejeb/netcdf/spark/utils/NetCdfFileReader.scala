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
package io.github.rejeb.netcdf.spark.utils

import io.github.rejeb.netcdf.spark.utils.NetCdfFileReader.openFile
import org.apache.spark.internal.Logging
import ucar.nc2.{NetcdfFile, NetcdfFiles}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.util.{Failure, Success, Try}

class NetCdfFileReader(path: String) extends Serializable with AutoCloseable with Logging {
  private val ncFilePool: ConcurrentLinkedQueue[NetcdfFile] = {
    val queue = new ConcurrentLinkedQueue[NetcdfFile]
    (0 until 2).foreach(_ => queue.add(openFile(path)))
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