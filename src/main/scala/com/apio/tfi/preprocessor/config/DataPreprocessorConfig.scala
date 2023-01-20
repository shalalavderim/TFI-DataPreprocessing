package com.apio.tfi.preprocessor.config

import com.typesafe.config.Config
import scala.collection.JavaConverters

/**
 * This case class holds the configuration for the ControllersSVUpdates.
 */
case class DataPreprocessorConfig(typeSafeConfig: Config) {
  /**
   * Execution Mode
   */
  private val executionModeKey = "preprocessor.execution.mode"
  val executionMode = typeSafeConfig.getString(executionModeKey)
  /**
   * Debug Mode
   */
  private val debugModeKey = "preprocessor.debug.mode"
  val debugMode = typeSafeConfig.getBoolean(debugModeKey)
  /**
   * Trucks Path
   */
  private val trucksPathKey = "preprocessor.trucks.path"
  val trucksPath = typeSafeConfig.getString(trucksPathKey)
  /**
   * Registrations Path
   */
  private val registrationsPathKey = "preprocessor.trucks.registrations.path"
  val registrationsPath = typeSafeConfig.getString(registrationsPathKey)
  /**
   * Trucks and Registrations Join Cols
   */
  private val trucksRegistrationsJoinColsKey = "preprocessor.trucks.registrations.joincols"
  val trucksRegistrationsJoinCols = typeSafeConfig.getStringList(trucksRegistrationsJoinColsKey)
  /**
   * Trucks and Registrations Join Type
   */
  private val trucksRegistrationsJoinTypeKey = "preprocessor.trucks.registrations.jointype"
  val trucksRegistrationsJoinType = typeSafeConfig.getString(trucksRegistrationsJoinTypeKey)
  /**
   *  Registrations Group Cols
   */
  private val registrationsGroupColsKey = "preprocessor.trucks.registrations.groupcols"
  val registrationsGroupCols = typeSafeConfig.getStringList(registrationsGroupColsKey)
  /**
   * Registrations RegistraionsDate ColName
   */
  private val registrationsRegDateColNameKey = "preprocessor.trucks.registrations.regdatecolname"
  val registrationsRegDateColName = typeSafeConfig.getString(registrationsRegDateColNameKey)
  /**
   * Registrations LicensePlate ColName
   */
  private val registrationsLPlateColNameKey = "preprocessor.trucks.registrations.lplatecolname"
  val registrationsLPlateColName = typeSafeConfig.getString(registrationsLPlateColNameKey)
  /**
   * LicensePlate ColName
   */
  private val licensePlateColNameKey = "preprocessor.trucks.licenseplate.colname"
  val licensePlateColName = typeSafeConfig.getString(licensePlateColNameKey)
  /**
   * LicensePlate CityColName
   */
  private val licensePlateCityColNameKey = "preprocessor.trucks.licenseplate.citycolname"
  val licensePlateCityColName = typeSafeConfig.getString(licensePlateCityColNameKey)
  /**
   * LicensePlate Delimiter
   */
  private val licensePlateDelimiterKey = "preprocessor.trucks.licenseplate.delimiter"
  val licensePlateDelimiter = typeSafeConfig.getString(licensePlateDelimiterKey)
  /**
   * LicensePlate Default
   */
  private val licensePlateDefaultValueKey = "preprocessor.trucks.licenseplate.defaultvalue"
  val licensePlateDefaultValue = typeSafeConfig.getString(licensePlateDefaultValueKey)
  /**
   * Category ColName
   */
  private val categoryColNameKey = "preprocessor.trucks.category.colname"
  val categoryColName = typeSafeConfig.getString(categoryColNameKey)
  /**
   * Category Tonnage ColName
   */
  private val categoryTonnageColNameKey = "preprocessor.trucks.category.tonnagecolname"
  val categoryTonnageColName = typeSafeConfig.getString(categoryTonnageColNameKey)
  /**
   * Category EngineSize ColName
   */
  private val categoryEngineSizeColNameKey = "preprocessor.trucks.category.enginesizecolname"
  val categoryEngineSizeColName = typeSafeConfig.getString(categoryEngineSizeColNameKey)
  /**
   * Category Tonnage Default Value
   */
  private val categoryTonnageDefaultValKey = "preprocessor.trucks.category.tonnagedefault"
  val categoryTonnageDefaultVal = typeSafeConfig.getInt(categoryTonnageDefaultValKey)
  /**
   * Category EngineSize Default Value
   */
  private val categoryEngineSizeDefaultValKey = "preprocessor.trucks.category.enginesizedefault"
  val categoryEngineSizeDefaultVal = typeSafeConfig.getInt(categoryEngineSizeDefaultValKey)
  /**
   * Category Small Tonnage Threshold
   */
  private val categorySmallTonnageThresholdKey = "preprocessor.trucks.category.smalltonagethreshold"
  val categorySmallTonnageThreshold = typeSafeConfig.getInt(categorySmallTonnageThresholdKey)
  /**
   * Category Small Engine Size Threshold
   */
  private val categorySmallEngineSizeThresholdKey = "preprocessor.trucks.category.smallenginesizethreshold"
  val categorySmallEngineSizeThreshold = typeSafeConfig.getInt(categorySmallEngineSizeThresholdKey)
  /**
   * Category Medium Tonnage Threshold
   */
  private val categoryMediumTonnageThresholdKey = "preprocessor.trucks.category.mediumtonagethreshold"
  val categoryMediumTonnageThreshold = typeSafeConfig.getInt(categoryMediumTonnageThresholdKey)
  /**
   * Category Medium Engine Size Threshold
   */
  private val categoryMediumEngineSizeThresholdKey = "preprocessor.trucks.category.mediumenginesizethreshold"
  val categoryMediumEngineSizeThreshold = typeSafeConfig.getInt(categoryMediumEngineSizeThresholdKey)
  /**
   * Category Small Name
   */
  private val categorySmallNameKey = "preprocessor.trucks.category.smallname"
  val categorySmallName = typeSafeConfig.getString(categorySmallNameKey)
  /**
   * Category Medium Name
   */
  private val categoryMediumNameKey = "preprocessor.trucks.category.mediumname"
  val categoryMediumName = typeSafeConfig.getString(categoryMediumNameKey)
  /**
   * Category Big Name
   */
  private val categoryBigNameKey = "preprocessor.trucks.category.bigname"
  val categoryBigName = typeSafeConfig.getString(categoryBigNameKey)

  /**
   * CityDictionary Input Path
   */
  private val cityDictPathKey = "preprocessor.trucks.citydict.path"
  val cityDictPath = typeSafeConfig.getString(cityDictPathKey)
  /**
   * CityDictionary Read Options
   */
  private val cityDictReadOptionsKey = "preprocessor.trucks.citydict.readoptions"
  val cityDictReadOptionsRawJava = typeSafeConfig.getStringList(cityDictReadOptionsKey)
  val cityDictReadOptionsRaw = JavaConverters.collectionAsScalaIterableConverter(cityDictReadOptionsRawJava).asScala.toSeq
  var cityDictReadOptions:Map[String,String] = Map()
  cityDictReadOptionsRaw.foreach(entry => {
    val key = entry.split(":")(0)
    val value = entry.split(":")(1)
    cityDictReadOptions = cityDictReadOptions ++ Map(key -> value)
  })
  /**
   * Trucks and CityDictionary Join Cols
   */
  private val cityDictJoinColsKey = "preprocessor.trucks.citydict.joincols"
  val cityDictJoinCols = typeSafeConfig.getStringList(cityDictJoinColsKey)
  /**
   * Trucks and CityDictionary Join Type
   */
  private val cityDictJoinTypeKey = "preprocessor.trucks.citydict.jointype"
  val cityDictJoinType = typeSafeConfig.getString(cityDictJoinTypeKey)
  /**
   * CityDictionary City ColName
   */
  private val cityDictCityColNameKey = "preprocessor.trucks.citydict.citycolname"
  val cityDictCityColName = typeSafeConfig.getString(cityDictCityColNameKey)
  /**
   * CityDictionary Value ColName
   */
  private val cityDictValueColNameKey = "preprocessor.trucks.citydict.valuecolname"
  val cityDictValueColName = typeSafeConfig.getString(cityDictValueColNameKey)
  /**
   * CityDictionary Property ColName
   */
  private val cityDictPropertyColNameKey = "preprocessor.trucks.citydict.prepertycolname"
  val cityDictPropertyColName = typeSafeConfig.getString(cityDictPropertyColNameKey)
  /**
   * CityDictionary Letter ColName
   */
  private val cityDictLetterColNameKey = "preprocessor.trucks.citydict.lettercolname"
  val cityDictLetterColName = typeSafeConfig.getString(cityDictLetterColNameKey)
  /**
   * CityDictionary Land ColName
   */
  private val cityDictLandColNameKey = "preprocessor.trucks.citydict.landcolname"
  val cityDictLandColName = typeSafeConfig.getString(cityDictLandColNameKey)
  /**
   * CityDictionary Land DefaultValue
   */
  private val cityDictLandDefaultValKey = "preprocessor.trucks.citydict.landdefaultval"
  val cityDictLandDefaultVal = typeSafeConfig.getString(cityDictLandDefaultValKey)
  /**
   * DaysBetween FirstRegistrationDate ColName
   */
  private val daysBetweenFirstRegColNameKey = "preprocessor.trucks.daysbetween.firstregdate"
  val daysBetweenFirstRegColName = typeSafeConfig.getString(daysBetweenFirstRegColNameKey)
  /**
   * DaysBetween RegistrationDate ColName
   */
  private val daysBetweenRegColNameKey = "preprocessor.trucks.daysbetween.regdate"
  val daysBetweenRegColName = typeSafeConfig.getString(daysBetweenRegColNameKey)
  /**
   * DaysBetween FirstAndReg ColName
   */
  private val daysBetweenFirstAndRegColNameKey = "preprocessor.trucks.daysbetween.firstandreg"
  val daysBetweenFirstAndRegColName = typeSafeConfig.getString(daysBetweenFirstAndRegColNameKey)
  /**
   * DaysBetween Today And FirstReg ColName
   */
  private val daysBetweenFromFirstRegColNameKey = "preprocessor.trucks.daysbetween.fromfirstreg"
  val daysBetweenFromFirstRegColName = typeSafeConfig.getString(daysBetweenFromFirstRegColNameKey)
  /**
   * DaysBetween Today And Reg ColName
   */
  private val daysBetweenFromRegColNameKey = "preprocessor.trucks.daysbetween.fromreg"
  val daysBetweenFromRegColName = typeSafeConfig.getString(daysBetweenFromRegColNameKey)
  /**
   * DaysBetween Date Format
   */
  private val daysBetweenDateFormatKey = "preprocessor.trucks.daysbetween.dateformat"
  val daysBetweenDateFormat = typeSafeConfig.getString(daysBetweenDateFormatKey)
  /**
   * IoT Data Path
   */
  private val iotDataPathKey = "preprocessor.iot.path"
  val iotDataPath = typeSafeConfig.getString(iotDataPathKey)
  /**
   * IoT Data Cols
   */
  private val iotDataColNamesKey = "preprocessor.iot.colnames"
  val iotDataColNames = typeSafeConfig.getStringList(iotDataColNamesKey)
  /**
   * IoT Data VIN ColName
   */
  private val iotDataVinColNameKey = "preprocessor.iot.vincolname"
  val iotDataVinColName = typeSafeConfig.getString(iotDataVinColNameKey)
  /**
   * IoT Data Cnt ColName
   */
  private val iotDataCntColNameKey = "preprocessor.iot.cntcolname"
  val iotDataCntColName = typeSafeConfig.getString(iotDataCntColNameKey)
  /**
   * IoT Data Alerts ColName
   */
  private val iotDataAlertsColNameKey = "preprocessor.iot.alertscolname"
  val iotDataAlertsColName = typeSafeConfig.getString(iotDataAlertsColNameKey)
  /**
   * IoT Data Alerts Threshold
   */
  private val iotDataAlertsThresholdKey = "preproecssor.iot.alertsthreshold"
  val iotDataAlertsThreshold = typeSafeConfig.getString(iotDataAlertsThresholdKey)
  /**
   * IoT Data Alerts Default value
   */
  private val iotDataAlertsDefaultValKey = "preproecssor.iot.alertsdefaultval"
  val iotDataAlertsDefaultVal = typeSafeConfig.getInt(iotDataAlertsDefaultValKey)
  /**
   * IoT Data Join Type
   */
  private val iotDataJoinTypeKey = "preproecssor.iot.jointype"
  val iotDataJoinType = typeSafeConfig.getString(iotDataJoinTypeKey)
  /**
   * Trucks Output Path
   */
  private val trucksOutputPathKey = "preprocessor.trucks.output.path"
  val trucksOutputPath = typeSafeConfig.getString(trucksOutputPathKey)
  /**
   * Trucks Partition Cols
   */
  private val trucksPartitionColsKey = "preprocessor.trucks.output.partition"
  val trucksPartitionCols = typeSafeConfig.getStringList(trucksPartitionColsKey)
  /**
   * Trucks Output Mode
   */
  private val trucksOutputModeKey = "preprocessor.trucks.output.mode"
  val trucksOutputMode = typeSafeConfig.getString(trucksOutputModeKey)
}

