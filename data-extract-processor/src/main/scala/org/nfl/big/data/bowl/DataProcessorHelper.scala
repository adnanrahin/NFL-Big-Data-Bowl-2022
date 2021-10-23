package org.nfl.big.data.bowl

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.nfl.big.data.bowl.constant.Constant

import scala.util.Try

object DataProcessorHelper {

  final def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + Constant.FILTER_DIR + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }

  final def isNumeric(num: String): Boolean = {

    def isShort(aString: String): Boolean = Try(aString.toLong).isSuccess

    def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess

    def isLong(aString: String): Boolean = Try(aString.toLong).isSuccess

    def isDouble(aString: String): Boolean = Try(aString.toDouble).isSuccess

    def isFloat(aString: String): Boolean = Try(aString.toFloat).isSuccess

    if (isShort(num) || isInt(num) || isLong(num) || isDouble(num) || isFloat(num)) true
    else false
  }

}
