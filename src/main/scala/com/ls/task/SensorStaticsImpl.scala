package com.ls.task

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

case class SensorDataComponent(sensorId: String, humidity: String)

case class MinAvgMaxHumidityComponent(sensorId: String, min: Any, mean: String, max: Any)

class SensorStaticsImpl {
  var listOfFiles: List[File] = List.empty
  var mean = ""
  var humidityListBuffer = new ListBuffer[List[String]]()
  var sensoridListBuffer = new ListBuffer[List[String]]()
  var map: Map[String, ListBuffer[Int]] = Map()

  def numOfProcessedFiles(dir: String): Int = {
    val filePath = new File(dir)
    if (filePath.exists && filePath.isDirectory) {
      listOfFiles = filePath.listFiles.filter(_.isFile).toList
      listOfFiles.size
    } else {
      List.empty.size
    }
  }

  def numOfProcessedMeasurements(): Int = {
    val conf = new SparkConf().setAppName("Reading Data From CSV Files").setMaster("local[*]")
    val sc = new SparkContext(conf)
    for (file <- listOfFiles) {
      val strRDD = sc.textFile(file.toString)
      val header = strRDD.first()
      val empRdd = strRDD.filter(row => row != header).map {
        line =>
          val sensorData = line.split(",")
          SensorDataComponent(sensorData(0), sensorData(1))
      }

      val humidityList = for {
        line <- empRdd
        a = line.humidity
      } yield a
      humidityListBuffer += humidityList.collect().toList

      val sensoridList = for {
        line <- empRdd
        a = line.sensorId
      } yield a

      sensoridListBuffer += sensoridList.collect().toList
    }
    val finalSenserIDList: List[String] = humidityListBuffer.toList.flatten
    finalSenserIDList.length
  }

  def numOfFailedMeasurements(): Int = {
    val HumidityListwithNaNData: List[String] = humidityListBuffer.toList.flatten
    var count = 0
    for (i <- humidityListBuffer.toList.flatten.indices) {
      if (HumidityListwithNaNData(i).equals("NaN")) {
        count = count + 1
      }
    }
    count
  }

  def minAvgMaxHumidity(): Iterable[MinAvgMaxHumidityComponent] = {
    val finalSenserIDList: List[String] = sensoridListBuffer.toList.flatten
    val finalHumidityList: List[Int] = humidityListBuffer.toList.flatten.map(e => if (e == "NaN") "0" else e).map(x => x.toInt)
    var count1 = 0
    for (strSensorId <- finalSenserIDList) {
      if (map.contains(strSensorId)) {
        map += (strSensorId -> (map(strSensorId) += finalHumidityList(count1)))
      } else {
        map += (strSensorId -> ListBuffer(finalHumidityList(count1)))
      }
      count1 = count1 + 1
    }
    println("sensorId" + "," + "min" + "," + "avg" + "," + "max :")
    val minAvgMaxHumidity = for (i <- map) yield {
      val remainder = i._2.filterNot(p => p.equals(0))
      val sum = remainder.sum
      var avg = 0
      if (remainder.nonEmpty || !sum.equals(0)) {
        avg = sum / remainder.size
      } else {
        avg = 0
      }

      mean = if (remainder.isEmpty) "NaN" else if (avg != 0) avg.toString else "NaN"
      val min = if (i._2.forall(p => p.equals(0))) "NaN" else remainder.min
      val max = if (i._2.forall(p => p.equals(0))) "NaN" else remainder.max
      MinAvgMaxHumidityComponent(i._1, min, mean, max)
    }
    minAvgMaxHumidity
  }

  def sortsSensorsByHighestAvgHumidity(): Unit = {
    var sortMap: Map[String, Int] = Map()
    for (i <- map) {
      val remainder = i._2.filterNot(p => p.equals(0))
      val sum = remainder.sum
      var avg = 0
      if (remainder.nonEmpty || !sum.equals(0)) {
        avg = sum / remainder.size
      } else {
        avg = 0
      }
      mean = if (remainder.isEmpty) "NaN" else if (avg != 0) avg.toString else "NaN"
      val mean1 = if (i._2 == List(0)) "NaN" else mean
      if (mean1 != "NaN") {
        sortMap += (i._1 -> mean1.toInt)
      }
      else {
        sortMap += (i._1 -> 0)
      }
    }

    print("sort sensors data by highest avg humidity :")
    for (i <- ListMap(sortMap.toSeq.sortWith(_._2 > _._2): _*)) {
      if (i._2 == 0) {
        print(i._1 -> "NaN" + ", ")
      } else {
        print(i._1 -> i._2 + ", ")
      }
    }
  }
}
