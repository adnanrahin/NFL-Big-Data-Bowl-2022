package org.nfl.big.data.bowl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

case class Games(gameId: String,
                 session: String,
                 week: String,
                 gameDate: String,
                 gameTimeEastern: String,
                 homeTeamAbbr: String,
                 visitorTeamAbbr: String
                ) extends LoadCsv {
  
  override def loadCSV(filePath: String, spark: SparkSession): RDD[Games] = {

    val gameCSV: RDD[String] = spark.sparkContext.textFile(filePath)

    val gamesRDD: RDD[Games] = gameCSV
      .map(row => row.split(",", -1))
      .map(str => Games(str(0),
        str(1), str(2), str(3), str(4), str(5), str(6))).mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    gamesRDD.cache().persist(StorageLevel.MEMORY_AND_DISK)

  }
}
