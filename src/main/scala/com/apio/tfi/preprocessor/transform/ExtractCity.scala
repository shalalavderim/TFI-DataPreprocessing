package com.apio.tfi.preprocessor.transform

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

/**
 * For a given license plate extracts the first characters which identify the city.
 */
case class ExtractCity(vConfig: DataPreprocessorConfig) {
  def apply(inputTable: DataFrame): DataFrame = {
    val extractCityUDF = udf(extractCity)
    inputTable.withColumn(vConfig.licensePlateCityColName, extractCityUDF(col(vConfig.licensePlateColName)))
  }

  def extractCity = (licensePlate: String) => {
    Try(licensePlate.split(vConfig.licensePlateDelimiter)(0)).getOrElse(vConfig.licensePlateDefaultValue)
  }
}
