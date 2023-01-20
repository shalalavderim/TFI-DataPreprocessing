package com.apio.tfi.job

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.job.DataPreprocessor
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.functions._

/**
 * TFI-DataPreprocessor Integration Test
 */
class DataPreprocessorSuite extends FunSuite with Matchers with DataFrameSuiteBase {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)
  val inputTruckDataPath = "src/test/resources/PreprocessorSuiteData/InputTrucks.parquet"
  val inputRegistrationsDataPath = "src/test/resources/PreprocessorSuiteData/InputRegistrations.parquet"
  val inputCityDictDataPath = "src/test/resources/PreprocessorSuiteData/InputCityDict.csv"
  val inputIoTDataPath = "src/test/resources/PreprocessorSuiteData/IoTData.json"
  val expectedDataPath = "src/test/resources/PreprocessorSuiteData/Output.parquet"

  test("DataPreprocessor Integration Test") {
    sc.setLogLevel("ERROR")
    val spark = sqlContext.sparkSession
    val preprocessorJob = new DataPreprocessor(vConfig, spark)

    val truckData = spark.read.parquet(inputTruckDataPath)
    if (vConfig.debugMode) {
      println("Integration Test Input Truck Data")
      truckData.show()
    }
    val registrationsData = spark.read.parquet(inputRegistrationsDataPath)
    if (vConfig.debugMode) {
      println("Integration Test Input Registrations Data")
      registrationsData.show()
    }
    val cityDictData = spark.read.options(vConfig.cityDictReadOptions).csv(inputCityDictDataPath)
    if (vConfig.debugMode) {
      println("Integration Test Input City Dictionary Data")
      cityDictData.show()
    }

    val iotData = spark.read.json(inputIoTDataPath)
    if (vConfig.debugMode) {
      println("Integration Test Input IoT Data")
      iotData.show()
    }

    val processedData = preprocessorJob.processData((truckData, registrationsData, cityDictData, iotData))
      .drop(vConfig.daysBetweenFromFirstRegColName)
      .drop(vConfig.daysBetweenFromRegColName)
      .withColumn(vConfig.iotDataAlertsColName, when(col(vConfig.iotDataAlertsColName).isNotNull, col(vConfig.iotDataAlertsColName)).otherwise(lit(null)))
      .withColumn(vConfig.cityDictLandColName, when(col(vConfig.cityDictLandColName).isNotNull, col(vConfig.cityDictLandColName)).otherwise(lit(null)))

    if (vConfig.debugMode) {
      println("Integration Test Output Master Data")
      processedData.show()
    }

    val expectedData = spark.read.parquet(expectedDataPath)
    assertDataFrameEquals(processedData, expectedData)
  }
}