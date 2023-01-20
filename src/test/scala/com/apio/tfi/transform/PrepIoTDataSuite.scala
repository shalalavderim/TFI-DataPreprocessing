package com.apio.tfi.transform

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.transform.PrepIoTData
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

/**
 * PrepIoTData Unit Test
 */
class PrepIoTDataSuite extends FunSuite with Matchers with DataFrameSuiteBase {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)
  val inputIoTDataPath = "src/test/resources/PrepIoTData/Input.json"
  val expectedIoTDataPath = "src/test/resources/PrepIoTData/Output.parquet"

  test("PrepIoTData Unit Test") {
    sc.setLogLevel("ERROR")
    val spark = sqlContext.sparkSession

    val inputData = spark.read.json(inputIoTDataPath)
    if (vConfig.debugMode) {
      println("PrepIotData Input Data")
      inputData.show()
    }

    val processedData = PrepIoTData(vConfig).apply(inputData)
    if (vConfig.debugMode) {
      println("PrepIotData Output Data")
      processedData.show()
    }

    val expectedData = spark.read.parquet(expectedIoTDataPath)
    assertDataFrameDataEquals(expectedData, processedData)
  }
}