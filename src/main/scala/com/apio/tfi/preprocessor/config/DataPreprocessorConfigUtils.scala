package com.apio.tfi.preprocessor.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters

/**
 * Utils used to read the config from file to a typeSafe Config instance.
 */
object DataPreprocessorConfigUtils {

  /**
   * Reads the config file in DBFS and converts it to typeSafe Config instance.
   * Example file: /mnt/ioee-sk-storage/AppConfig/SKParser.conf
   */
  def readConfigFromDBFS(inputPath: String): Config = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    val confStr = dbutils.fs.head(inputPath)

    ConfigFactory.parseString(confStr)
  }
  /**
   * Converts a given list of strings in java to a list of string in Scala.
   */
  def toScalaList(javaList: java.util.List[String]): Seq[String] = {
    JavaConverters.collectionAsScalaIterableConverter(javaList).asScala.toSeq
  }
}
