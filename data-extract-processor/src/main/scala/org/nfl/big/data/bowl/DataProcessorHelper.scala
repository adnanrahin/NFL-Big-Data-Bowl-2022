package org.nfl.big.data.bowl

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.nfl.big.data.bowl.constant.Constant

object DataProcessorHelper {

  def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + Constant.FILTER_DIR + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }

}
