package com.apio.tfi.infrastructure

import org.apache.spark.sql.SparkSession

/**
 * Contains the building blocks for a databricks app.
 */
trait DatabricksApp extends App {

  val spark = SparkSession.builder().getOrCreate()

}
