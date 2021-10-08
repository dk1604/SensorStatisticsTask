package com.ls.task

object ReadSensorData {
  def main(args: Array[String]): Unit = {
    val path = "src/main/resources"

    val sensorSpark = new SensorStaticsImpl()
    val noOfProcessedFile = sensorSpark.numOfProcessedFiles(path)
    println("Number of processed files:" + noOfProcessedFile)

    val numOfProcessedMeasure = sensorSpark.numOfProcessedMeasurements()
    println("Number of processed measurements: " + numOfProcessedMeasure)

    val numOfFailedMeasure = sensorSpark.numOfFailedMeasurements()
    println("Number of failed measurements: " + numOfFailedMeasure)

    val minAvgMaxHumidity = sensorSpark.minAvgMaxHumidity()
    minAvgMaxHumidity.foreach(data => println(data.sensorId + "," + data.min + "," + data.mean + "," + data.max))

    sensorSpark.sortsSensorsByHighestAvgHumidity()
  }
}
