package com.apio.tfi.preprocessor.transform

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

/**
 * For a given first registration date and registration date calculates the new columns:
 *  DaysBetweenFirstRegAndReg: Days between the first registration date and registration date.
 *  DaysFromFirstReg: Days between today and first registration date.
 *  DaysFromReg: Days between today and first registration date.
 * Also the input columns FirstRegDate and RegistrationDate are casted to Date data type.
 */
case class DaysBetween(vConfig: DataPreprocessorConfig) {
  def apply(inputTable: DataFrame): DataFrame = {
    inputTable
      .withColumn(vConfig.daysBetweenFirstRegColName, to_date(col(vConfig.daysBetweenFirstRegColName),vConfig.daysBetweenDateFormat))
      .withColumn(vConfig.daysBetweenRegColName, to_date(col(vConfig.daysBetweenRegColName),vConfig.daysBetweenDateFormat))
      .withColumn(vConfig.daysBetweenFirstAndRegColName, datediff(col(vConfig.daysBetweenRegColName), col(vConfig.daysBetweenFirstRegColName)))
      .withColumn(vConfig.daysBetweenFromFirstRegColName, datediff(current_date(), col(vConfig.daysBetweenFirstRegColName)))
      .withColumn(vConfig.daysBetweenFromRegColName, datediff(current_date(), col(vConfig.daysBetweenRegColName)))
  }
}
