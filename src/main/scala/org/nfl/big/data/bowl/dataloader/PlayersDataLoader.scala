package org.nfl.big.data.bowl.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.DataLoader
import org.nfl.big.data.bowl.entity.Players

class PlayersDataLoader(filePath: String, spark: SparkSession) extends DataLoader{

  override def loadRDD(): RDD[Players] = {

    val playersDataCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val playersDataRDD: RDD[Players] = playersDataCSV
      .map(row => row.split(",", -1))
      .map(str => Players(str(0),
        str(1), str(2), str(3), str(4), str(5), str(6))).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    playersDataRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }
}
