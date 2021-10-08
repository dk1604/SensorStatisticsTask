package com.ls.task

trait SensorStatistics {
  def numOfProcessedFiles(path: String): Int

  def numOfProcessedMeasurements(): Int

  def numOfFailedMeasurements():Int

  def minAvgMaxHumidity(): Iterable[MinAvgMaxHumidityComponent]

  def sortsSensorsByHighestAvgHumidity():Unit
}
