package org.nfl.big.data.bowl.dataextractors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.DataProcessorHelper.isNumeric
import org.nfl.big.data.bowl.entity.PFFScoutingData

object PFFScoutingDataExtractor {

  final val NA = "NA"

  def extractPuntRushers(pffScoutingRDD: RDD[PFFScoutingData]): RDD[(String, List[String])] = {

    val result: RDD[(String, List[String])] =
      pffScoutingRDD
        .map(pf => (pf.gameId, pf.puntRushers))
        .groupBy(_._1)
        .map {
          t => {
            (t._1, t._2.toList)
          }
        }
        .map {
          t => {
            (t._1,
              t._2.filter(f => !f._2.equalsIgnoreCase(NA))
                .map(f => f._2)
            )
          }
        }

    result.foreach(f => println(f._1 + " => " + f._2))

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def findTotalHangTimeInEachGame(pffScoutingRDD: RDD[PFFScoutingData]): RDD[(String, String)] = {

    val result: RDD[(String, String)] =
      pffScoutingRDD.filter(pf => !pf.hangTime.equalsIgnoreCase(NA))
        .filter(t => isNumeric(t.hangTime))
        .map(pf => (pf.gameId, pf.hangTime.toDouble))
        .groupBy(_._1)
        .map {
          t => {
            (t._1
              , BigDecimal
              .apply(t._2.foldLeft(0.0)(_ + _._2))
              .setScale(6, BigDecimal.RoundingMode.HALF_EVEN)
              .toDouble)
          }
        }
        .sortBy(_._2)
        .map(kv => (kv._1, kv._2.toString))

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def findTotalDistanceRunInEachGameToDf(pffScoutingRDD: RDD[PFFScoutingData], spark: SparkSession): DataFrame = {

    val distanceRDD: RDD[(String, String)] = findTotalHangTimeInEachGame(pffScoutingRDD = pffScoutingRDD)

    spark
      .createDataFrame(distanceRDD)
      .toDF("gameId", "totalHangingTime")
  }

  def puntRushersToDF(pffScoutingRDD: RDD[PFFScoutingData], spark: SparkSession): DataFrame = {

    val puntRushers = extractPuntRushers(pffScoutingRDD)

    spark
      .createDataFrame(puntRushers)
      .toDF("gameID", "puntRushers")
  }

}
