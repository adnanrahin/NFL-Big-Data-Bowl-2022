package org.nfl.big.data.bowl.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.entity.Players

class PlayersDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Players] = {

    val playersDataCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val playersDataRDD: RDD[Players] = playersDataCSV
      .map(row => row.split(",", -1))
      .map(str =>
        Players(
          str(0).replace("\"", ""),
          str(1).replace("\"", ""),
          str(2).replace("\"", ""),
          str(3).replace("\"", ""),
          str(4).replace("\"", ""),
          str(5).replace("\"", ""),
          str(6).replace("\"", "")
        )
      )
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }

    playersDataRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }
}
