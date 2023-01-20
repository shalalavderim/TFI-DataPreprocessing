package com.apio.tfi.transform

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.transform.PrepCityDict
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

/**
 * PrepCityDict Unit Test
 */
class PrepCityDictSuite extends FunSuite with Matchers with DataFrameSuiteBase {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)
  val inputCityInfoPath = "src/test/resources/PrepCityDictData/Input.csv"
  val expectedCityDictPath = "src/test/resources/PrepCityDictData/Output.parquet"

  test("PrepCityDict Unit Test") {
    sc.setLogLevel("ERROR")
    val spark = sqlContext.sparkSession

    val inputData = spark.read.options(vConfig.cityDictReadOptions).csv(inputCityInfoPath)
    if (vConfig.debugMode) {
      println("PrepCity Input Data")
      inputData.show()
    }

    val processedData = PrepCityDict(vConfig).apply(inputData)
    if (vConfig.debugMode) {
      println("PrepCity Output Data")
      processedData.show()
    }

    val expectedData = spark.read.parquet(expectedCityDictPath)
    assertDataFrameEquals(processedData, expectedData)
  }
}