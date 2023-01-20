package com.apio.tfi.transform

import java.io.File

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import com.apio.tfi.preprocessor.transform.AssignCategory
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

/**
 * Unit test for AssignCity transformer
 */
class AssignCategorySuite extends FunSuite with Matchers {

  val configPath = getClass.getResource("/TFI-DataPreprocessor.conf").getPath
  val configFile = new File(configPath)
  val tsConfig = ConfigFactory.parseFile(configFile)
  val vConfig = DataPreprocessorConfig(tsConfig)

  val tc1Tonnage = -1
  val tc1EngineSize = -1
  val tc2Tonnage = 0
  val tc2EngineSize = 0
  val tc3Tonnage = 2
  val tc3EngineSize = 3000
  val tc4Tonnage = 5
  val tc4EngineSize = 3000
  val tc5Tonnage = 2
  val tc5EngineSize = 5000
  val tc6Tonnage = 4
  val tc6EngineSize = 5000
  val tc7Tonnage = 11
  val tc7EngineSize = 5000
  val tc8Tonnage = 15
  val tc8EngineSize = 7000

  val expectedTc1 = vConfig.categorySmallName
  val expectedTc2 = vConfig.categorySmallName
  val expectedTc3 = vConfig.categorySmallName
  val expectedTc4 = vConfig.categoryMediumName
  val expectedTc5 = vConfig.categoryMediumName
  val expectedTc6 = vConfig.categoryMediumName
  val expectedTc7 = vConfig.categoryBigName
  val expectedTc8 = vConfig.categoryBigName

  test("AssignCity Unit Test") {
    val res1 = AssignCategory(vConfig).assignCategory(tc1Tonnage, tc1EngineSize)
    val res2 = AssignCategory(vConfig).assignCategory(tc2Tonnage, tc2EngineSize)
    val res3 = AssignCategory(vConfig).assignCategory(tc3Tonnage, tc3EngineSize)
    val res4 = AssignCategory(vConfig).assignCategory(tc4Tonnage, tc4EngineSize)
    val res5 = AssignCategory(vConfig).assignCategory(tc5Tonnage, tc5EngineSize)
    val res6 = AssignCategory(vConfig).assignCategory(tc6Tonnage, tc6EngineSize)
    val res7 = AssignCategory(vConfig).assignCategory(tc7Tonnage, tc7EngineSize)
    val res8 = AssignCategory(vConfig).assignCategory(tc8Tonnage, tc8EngineSize)

    assertResult(expectedTc1)(res1)
    assertResult(expectedTc2)(res2)
    assertResult(expectedTc3)(res3)
    assertResult(expectedTc4)(res4)
    assertResult(expectedTc5)(res5)
    assertResult(expectedTc6)(res6)
    assertResult(expectedTc7)(res7)
    assertResult(expectedTc8)(res8)
  }
}