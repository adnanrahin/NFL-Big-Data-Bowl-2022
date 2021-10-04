package org.nfl.big.data.bowl.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.DataLoader
import org.nfl.big.data.bowl.entity.PFFScoutingData

class PFFScoutingDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[PFFScoutingData] = {

    val pffScoutingDataCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val pffScoutingDataRDD: RDD[PFFScoutingData] = pffScoutingDataCSV
      .map(row => row.split(",", -1))
      .map(str => PFFScoutingData(str(0),
        str(1), str(2), str(3), str(4), str(5), str(6),
        str(7), str(8), str(9), str(10), str(1), str(12),
        str(13), str(14), str(15), str(16), str(17),
        str(18), str(19))).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    pffScoutingDataRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }
}
