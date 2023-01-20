package com.apio.tfi.preprocessor.transform

import com.apio.tfi.preprocessor.config.{DataPreprocessorConfig, DataPreprocessorConfigUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Aggregate the input Truck Registrations by considering for each truck identified with Vin
 * the latest licence plate number and the earliest registration date.
 */
case class PrepRegistrations(vConfig: DataPreprocessorConfig) {

  def apply(inputTable: DataFrame): DataFrame = {
    inputTable
      .orderBy(vConfig.registrationsRegDateColName)
      .groupBy(DataPreprocessorConfigUtils.toScalaList(vConfig.registrationsGroupCols).map(col(_)): _*)
      .agg(last(vConfig.registrationsLPlateColName).as(vConfig.registrationsLPlateColName),
        first(vConfig.registrationsRegDateColName).as(vConfig.registrationsRegDateColName))
  }
}
