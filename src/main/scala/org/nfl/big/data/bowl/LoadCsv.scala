package org.nfl.big.data.bowl

import org.apache.spark.sql.SparkSession

trait LoadCsv {

  def loadCSV(filePath: String, spark: SparkSession): Any

}
