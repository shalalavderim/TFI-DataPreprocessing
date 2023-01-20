package com.apio.tfi.transform

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.transform.DaysBetween
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

/**
 * DaysBetween Unit Test
 * The test skips checking of the columns which calculate the number of  days between today and another date.
 */
class DaysBetweenSuite extends FunSuite with Matchers with DataFrameSuiteBase {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)
  val inputDaysBetweenInputPath = "src/test/resources/DaysBetweenData/Input.parquet"
  val expectedDaysBetweenPath = "src/test/resources/DaysBetweenData/Output.parquet"

  test("DaysBetween Unit Test") {
    sc.setLogLevel("ERROR")
    val spark = sqlContext.sparkSession

    val inputData = spark.read.parquet(inputDaysBetweenInputPath)
    if (vConfig.debugMode) {
      println("DaysBetween Input Data")
      inputData.show()
    }

    val processedData = DaysBetween(vConfig).apply(inputData)
      .drop(vConfig.daysBetweenFromFirstRegColName)
      .drop(vConfig.daysBetweenFromRegColName)
    if (vConfig.debugMode) {
      println("DaysBetween Output Data")
      processedData.show()
    }

    val expectedData = spark.read.parquet(expectedDaysBetweenPath)
    assertDataFrameEquals(expectedData, processedData)
  }
}