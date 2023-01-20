package com.apio.tfi.preprocessor

import com.apio.tfi.infrastructure.DatabricksApp
import com.apio.tfi.preprocessor.config.{DataPreprocessorConfig, DataPreprocessorConfigUtils}
import com.apio.tfi.preprocessor.job.DataPreprocessor

/**
 * This is the main class of the executable jar.
 * It orchestrates calling of other components of the job.
 */
object DataPreprocessorBootstrap extends DatabricksApp {
  println("Reading Configuration ...")
  val tsConfig = DataPreprocessorConfigUtils.readConfigFromDBFS(args(0))
  val vConfig = DataPreprocessorConfig(tsConfig)

  println(s"Preprocessing Raw Data ...")
  val dataPreprocessor = new DataPreprocessor(vConfig, spark)
  dataPreprocessor.writeData(dataPreprocessor.processData(dataPreprocessor.readData()))

}