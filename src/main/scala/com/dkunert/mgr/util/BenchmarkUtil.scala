package com.dkunert.mgr.util

object BenchmarkUtil {

  var processingTime:Long = 0

  def startTime(): Unit = {
    processingTime = System.nanoTime()
  }

  def getProcessingTime(): Long = {
    return System.nanoTime() - processingTime
  }
}
