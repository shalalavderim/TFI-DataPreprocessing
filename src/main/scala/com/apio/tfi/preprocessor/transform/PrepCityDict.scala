package com.apio.tfi.preprocessor.transform

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Transform and Pivot the City Information to a Dictionary of City Letter and Land.
 */
case class PrepCityDict(vConfig: DataPreprocessorConfig) {
  def apply(inputTable: DataFrame): DataFrame = {
    val pivotedCityDf = inputTable
      .groupBy(vConfig.cityDictCityColName)
      .pivot(vConfig.cityDictPropertyColName)
      .agg(first(vConfig.cityDictValueColName))

    pivotedCityDf
      .drop(vConfig.cityDictCityColName)
      .withColumnRenamed(vConfig.cityDictLetterColName, vConfig.cityDictCityColName)
  }
}
