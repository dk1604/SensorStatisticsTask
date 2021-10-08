package com.ls.task

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import java.io.File
import scala.collection.mutable.ListBuffer

class SensorStaticsImplTest extends FunSuite with Matchers with MockitoSugar {
  val actualPath = "src/test/resources"
  val emptyPath = ""
  val mockSensorStaticsImpl = new SensorStaticsImpl()
  val testList = List(new File("src/main/resources/leader-1.csv"))
  val testHumidityListBuffer = ListBuffer(List("10", "88", "NaN", "80", "NaN", "78", "98"))
  test("number of processed file should be 1") {
    assert(mockSensorStaticsImpl.numOfProcessedFiles(actualPath).equals(1))
  }

  test("number of processed file should be empty") {
    assert(mockSensorStaticsImpl.numOfProcessedFiles(emptyPath).equals(0))
  }

  test("number of processed measurements shoul be 3") {
    val testConf = new SparkConf().setAppName("TestApp").setMaster("local[*]")
    val testSC = mock[SparkContext]
    mockSensorStaticsImpl.listOfFiles = testList
    assert(mockSensorStaticsImpl.numOfProcessedMeasurements.equals(3))
  }

  test("number of failed measurements should be 1") {
    mockSensorStaticsImpl.humidityListBuffer = testHumidityListBuffer
    assert(mockSensorStaticsImpl.numOfFailedMeasurements().equals(2))
  }
}
