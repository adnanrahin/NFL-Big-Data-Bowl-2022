package org.nfl.big.data.bowl.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.DataLoader
import org.nfl.big.data.bowl.entity.Games

class GameDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Games] = {

    val gameCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val gamesRDD: RDD[Games] = gameCSV
      .map(row => row.split(",", -1))
      .map(str =>
        Games(
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

    gamesRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }
}
