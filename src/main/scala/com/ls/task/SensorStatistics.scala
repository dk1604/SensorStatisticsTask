package com.ls.task

trait SensorStatistics {
  def numOfProcessedFiles(path: String): Int

  def numOfProcessedMeasurements(): Int

  def numOfFailedMeasurements():Int

  def minAvgMaxHumidity(): Unit

  def sortsSensorsByHighestAvgHumidity():Unit
}
