package com.apio.tfi.transform

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.transform.ExtractCity
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

/**
 * Unit test for ExtractCity transformer
 */
class ExtractCitySuite extends FunSuite with Matchers {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)

  val tc1 = null
  val tc2 = ""
  val tc3 = "B-LM-123"
  val tc4 = "MH-DX-620"
  val tc5 = "D-654"

  val expectedTc1 = "Unknown"
  val expectedTc2 = ""
  val expectedTc3 = "B"
  val expectedTc4 = "MH"
  val expectedTc5 = "D"

  test("ExtractCity Unit Test") {
    val res1 = ExtractCity(vConfig).extractCity(tc1)
    val res2 = ExtractCity(vConfig).extractCity(tc2)
    val res3 = ExtractCity(vConfig).extractCity(tc3)
    val res4 = ExtractCity(vConfig).extractCity(tc4)
    val res5 = ExtractCity(vConfig).extractCity(tc5)

    assertResult(expectedTc1)(res1)
    assertResult(expectedTc2)(res2)
    assertResult(expectedTc3)(res3)
    assertResult(expectedTc4)(res4)
    assertResult(expectedTc5)(res5)
  }
}