package com.apio.tfi.preprocessor.transform

import com.apio.tfi.preprocessor.config.DataPreprocessorConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * For a given tonnage and engineSize assigns the truck category.
 */
case class AssignCategory(vConfig: DataPreprocessorConfig) {
  def apply(inputTable: DataFrame): DataFrame = {
    val assignCategoryUDF = udf(assignCategory)
    inputTable.withColumn(vConfig.categoryColName,
      assignCategoryUDF(col(vConfig.categoryTonnageColName), col(vConfig.categoryEngineSizeColName)))
  }

  def assignCategory = (tonnage: Int, engineSize: Int) => {
    val pTonnage = Option(tonnage).getOrElse(vConfig.categoryTonnageDefaultVal)
    val pEngineSize = Option(engineSize).getOrElse(vConfig.categoryEngineSizeDefaultVal)

    if (pTonnage < vConfig.categorySmallTonnageThreshold && pEngineSize < vConfig.categorySmallEngineSizeThreshold)
      vConfig.categorySmallName
    else if (pTonnage < vConfig.categoryMediumTonnageThreshold && pEngineSize < vConfig.categoryMediumEngineSizeThreshold)
      vConfig.categoryMediumName
    else
      vConfig.categoryBigName
  }
}
