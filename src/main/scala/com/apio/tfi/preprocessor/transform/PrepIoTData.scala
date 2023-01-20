package com.apio.tfi.preprocessor.transform

import com.apio.tfi.preprocessor.config.{DataPreprocessorConfig, DataPreprocessorConfigUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Prepares the IotData by counting the number of alerts for each truck identified by VIN.
 */
case class PrepIoTData(vConfig: DataPreprocessorConfig) {
  def apply(iotDataDf: DataFrame): DataFrame = {
    val selectedData = iotDataDf.select(DataPreprocessorConfigUtils.toScalaList(vConfig.iotDataColNames).map(col): _*)
    val filteredData = selectedData.filter(vConfig.iotDataAlertsThreshold)
    val groupedData = filteredData.groupBy(vConfig.iotDataVinColName).count()
      .withColumnRenamed(vConfig.iotDataCntColName, vConfig.iotDataAlertsColName)
      .withColumnRenamed(vConfig.iotDataVinColName, vConfig.iotDataVinColName.toUpperCase)

    groupedData
  }
}
