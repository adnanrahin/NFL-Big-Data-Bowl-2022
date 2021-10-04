package org.nfl.big.data.bowl.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.DataLoader
import org.nfl.big.data.bowl.entity.Games

class GameDataLoader(fileName: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Games] = {

    val gameCSV: RDD[String] = this.spark.sparkContext.textFile(this.fileName)

    val gamesRDD: RDD[Games] = gameCSV
      .map(row => row.split(",", -1))
      .map(str => Games(str(0),
        str(1), str(2), str(3), str(4), str(5), str(6))).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    gamesRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }
}
