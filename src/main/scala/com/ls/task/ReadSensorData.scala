package com.ls.task

object ReadSensorData {
  def main(args: Array[String]): Unit = {
    val path = "src/main/resources"

    val sensorSpark=new SensorStaticsImpl()
    val noOfProcessedFile=sensorSpark.numOfProcessedFiles(path)
    println("Num of processed files:" + noOfProcessedFile)
    val numOfProcessedMeasure=sensorSpark.numOfProcessedMeasurements()
    println("Num of processed measurements: "+numOfProcessedMeasure)
    val numOfFailedMeasure=sensorSpark.numOfFailedMeasurements()
    println("Num of failed measurements: "+numOfFailedMeasure)
    sensorSpark.minAvgMaxHumidity()
    sensorSpark.sortsSensorsByHighestAvgHumidity()
  }
}
