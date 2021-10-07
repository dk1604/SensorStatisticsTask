package com.ls.task

import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.ListMap

case class SensorDataComponent(sensorId: String, humidity: String)

class SensorStaticsImpl {
  var listOfFiles: List[File] = null
  var mean = ""
  var humidityListBuffer = new ListBuffer[List[String]]()
  var sensoridListBuffer = new ListBuffer[List[String]]()
  var map: Map[String, ListBuffer[Int]] = Map()

  def numOfProcessedFiles(dir: String): Int = {
    val filePath = new File(dir)
    filePath.exists && filePath.isDirectory match {
      case true => listOfFiles = filePath.listFiles.filter(_.isFile).toList
        listOfFiles.size
      case false => List.empty.size
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
    val finalSenserIDList: List[String] = sensoridListBuffer.toList.flatten
    finalSenserIDList.length
  }

  def numOfFailedMeasurements(): Int = {
    val HumidityListwithNaNData: List[String] = humidityListBuffer.toList.flatten
    var count = 0
    for (i <- 0 to (humidityListBuffer.toList.flatten.length - 1)) {
      HumidityListwithNaNData(i).equals("NaN") match {
        case true => count += count
        case false => count
      }
    }
    count
  }

  def minAvgMaxHumidity(): Unit = {
    val finalSenserIDList: List[String] = sensoridListBuffer.toList.flatten
    val finalHumidityList: List[Int] = humidityListBuffer.toList.flatten.map(e => if (e == "NaN") "0" else e).map(x => x.toInt)
    var count = 0
    for (strSensorId <- finalSenserIDList) {
      map.contains(strSensorId) match {
        case true => map += (strSensorId -> (map(strSensorId) += finalHumidityList(count)))
        case false => map += (strSensorId -> ListBuffer(finalHumidityList(count)))
      }
      count += count
    }
    println("sensorId" + "," + "min" + "," + "avg" + "," + "max :")
    for (i <- map) {
      val remainder = i._2.filterNot(p => p.equals(0))
      val sum = (i._2.filterNot(p => p.equals(0)).sum)
      var avg = 0
      (!remainder.isEmpty || !sum.equals(0)) match {
        case true => avg = (sum / remainder.size)
        case false => avg = 0
      }

      mean = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else if (avg != 0) avg.toString else "NaN"
      val min = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else i._2.filterNot(p => p.equals(0)).min
      val max = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else i._2.filterNot(p => p.equals(0)).max
      println(i._1 + "," + min + "," + mean + "," + max)
    }
  }

  def sortsSensorsByHighestAvgHumidity(): Unit = {
    var sortMap: Map[String, Int] = Map()
    for (i <- map) {
      val remainder = i._2.filterNot(p => p.equals(0))
      val sum = i._2.filterNot(p => p.equals(0)).sum
      var avg = 0
      if (!remainder.isEmpty || !sum.equals(0)) {
        avg = (sum / remainder.size)
      } else {
        avg = 0
      }
      mean = if (i._2.filterNot(p => p.equals(0)).isEmpty) "NaN" else if (avg != 0) avg.toString else "NaN"

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
      (i._2 == 0) match {
        case true => print(i._1 -> "NAN" + ", ")
        case false => print(i._1 -> i._2 + ", ")
      }
    }
  }
}
