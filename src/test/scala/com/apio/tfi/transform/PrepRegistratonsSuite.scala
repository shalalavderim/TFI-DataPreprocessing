package com.apio.tfi.transform

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.transform.PrepRegistrations
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

/**
 * PrepRegistrations Unit Test
 */
class PrepRegistratonsSuite extends FunSuite with Matchers with DataFrameSuiteBase {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)
  val inputRegistrationsInfoPath = "src/test/resources/PrepRegistrationsData/Input.parquet"
  val expectedRegistrationsInfoPath = "src/test/resources/PrepRegistrationsData/Output.parquet"

  test("PrepRegistrations Unit Test") {
    sc.setLogLevel("ERROR")
    val spark = sqlContext.sparkSession

    val inputData = spark.read.parquet(inputRegistrationsInfoPath)
    if (vConfig.debugMode) {
      println("PrepRegistrations Input Data")
      inputData.show()
    }

    val processedData = PrepRegistrations(vConfig).apply(inputData)
    if (vConfig.debugMode) {
      println("PrepRegistrations Output Data")
      processedData.show()
    }

    val expectedData = spark.read.parquet(expectedRegistrationsInfoPath)
    assertDataFrameEquals(processedData, expectedData)
  }
}