package com.apio.tfi.preprocessor.job

import com.apio.tfi.preprocessor.config.{DataPreprocessorConfig, DataPreprocessorConfigUtils}
import com.apio.tfi.preprocessor.transform._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * ETL Job which reads the raw relevant data, combines, transforms them, and writes to master data.
 */
class DataPreprocessor(vConfig: DataPreprocessorConfig, spark: SparkSession) extends Serializable {

  def readData(): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    println("Read Raw Data ...")
    val trucksDf = spark.read.parquet(vConfig.trucksPath)
    val registrationsDf = spark.read.parquet(vConfig.registrationsPath)
    val cityInfoDf = spark.read.options(vConfig.cityDictReadOptions).csv(vConfig.cityDictPath)
    val iotDataDf = spark.read.json(vConfig.iotDataPath)

    println("Read Raw Data Success.")
    println(s"IotData Size: ${iotDataDf.count()}")
    (trucksDf, registrationsDf, cityInfoDf, iotDataDf)
  }

  def processData(inputData: (DataFrame, DataFrame, DataFrame, DataFrame)): DataFrame = {
    println("Transform Raw Data ...")
    val trucksDf = inputData._1
    val registrationsDf = inputData._2
    val cityInfoDf = inputData._3
    val iotDataDf = inputData._4

    val stage1Data = PrepRegistrations(vConfig).apply(registrationsDf)
    val stage2Data = trucksDf.join(stage1Data,
      DataPreprocessorConfigUtils.toScalaList(vConfig.trucksRegistrationsJoinCols), vConfig.trucksRegistrationsJoinType)
    val stage3Data = ExtractCity(vConfig).apply(stage2Data)
    val stage4Data = AssignCategory(vConfig).apply(stage3Data)
    val stage5Data = PrepCityDict(vConfig).apply(cityInfoDf)
    val stage6Data = stage4Data.join(stage5Data,
      DataPreprocessorConfigUtils.toScalaList(vConfig.cityDictJoinCols), vConfig.cityDictJoinType)
    val stage7Data = stage6Data.na.fill(vConfig.cityDictLandDefaultVal, Seq(vConfig.cityDictLandColName))
    val stage8Data = DaysBetween(vConfig).apply(stage7Data)
    val stage9Data = PrepIoTData(vConfig).apply(iotDataDf)
    val stage10Data = stage8Data.join(stage9Data, Seq(vConfig.iotDataVinColName.toUpperCase()), vConfig.iotDataJoinType)
    val stage11Data = stage10Data.na.fill(vConfig.iotDataAlertsDefaultVal, Seq(vConfig.iotDataAlertsColName))

    println("Transform Raw Data Success.")
    stage11Data
  }

  def writeData(processedData: DataFrame): Unit = {
    println("Writing Processed Master Data ...")
    if (vConfig.debugMode) {
      println(s"Partition Cols: ${vConfig.trucksPartitionCols}")
      println(s"Output Path: ${vConfig.trucksOutputPath}")
    }

    processedData
      .repartition(DataPreprocessorConfigUtils.toScalaList(vConfig.trucksPartitionCols).map(c => col(c)): _*)
      .write.mode(vConfig.trucksOutputMode).partitionBy(DataPreprocessorConfigUtils.toScalaList(vConfig.trucksPartitionCols): _*)
      .parquet(vConfig.trucksOutputPath)

    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    dbutils.fs.rm(s"${vConfig.trucksOutputPath}/_SUCCESS")
    println("Write Processed Master Data Success.")
  }
}
