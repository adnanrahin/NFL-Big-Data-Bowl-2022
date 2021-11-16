package org.nfl.big.data.bowl.dataextractors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.DataProcessorHelper.isNumeric
import org.nfl.big.data.bowl.constant.Constant._
import org.nfl.big.data.bowl.entity.PFFScoutingData

object PFFScoutingDataExtractor {

  private def kickDirectionMissMatchExtractWithGameId(pffScoutingRDD: RDD[PFFScoutingData]): RDD[(String, String)] = {

    val filterData = pffScoutingRDD
      .filter(t => !t.kickDirectionIntended.equalsIgnoreCase(NOT_AVAILABLE)
        && !t.kickDirectionActual.equalsIgnoreCase(NOT_AVAILABLE))

    val result: RDD[(String, String)] =
      filterData
        .map(t => (t.gameId, t.kickDirectionActual, t.kickDirectionIntended))
        .groupBy(t => t._1)
        .map {
          t =>
            (t._1,
              t._2
                .toList
                .filter(t => !t._2.equalsIgnoreCase(t._3))
                .map(f => (f._2, f._3))
                .flatMap {
                  t => t._1 :: t._2 :: Nil
                }
                .mkString
                .groupBy(identity)
                .mapValues(_.length)
                .toList
                .map {
                  str =>
                    val kickDirection = str._1
                    val count = str._2
                    s"$kickDirection $COLON_DELIMITER $count"
                }.mkString(PIPE_DELIMITER)
            )
        }
        .filter(t => t._2.nonEmpty)

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def kickDirectionMissMatchExtractToDf(pffScoutingRDD: RDD[PFFScoutingData], spark: SparkSession): DataFrame = {

    val kickDirectionMissMatch: RDD[(String, String)] = kickDirectionMissMatchExtractWithGameId(pffScoutingRDD = pffScoutingRDD)

    spark
      .createDataFrame(kickDirectionMissMatch)
      .toDF("gameId", "KickDirections")
  }

  private def extractPuntRushers(pffScoutingRDD: RDD[PFFScoutingData]): RDD[(String, String)] = {

    val result: RDD[(String, String)] =
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
              t._2.filter(f => !f._2.equalsIgnoreCase(NOT_AVAILABLE))
                .map {
                  f => f._2.replace(SEMICOLON_DELIMITER, COMMA_DELIMITER)
                }.mkString(COMMA_DELIMITER)
                .split(COMMA_DELIMITER)
                .toList
                .map(str => str.trim).mkString(PIPE_DELIMITER)
            )
          }
        }

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def puntRushersToDF(pffScoutingRDD: RDD[PFFScoutingData], spark: SparkSession): DataFrame = {

    val puntRushers = extractPuntRushers(pffScoutingRDD)

    spark
      .createDataFrame(puntRushers)
      .toDF("gameID", "puntRushers")
  }

  private def findTotalHangTimeInEachGame(pffScoutingRDD: RDD[PFFScoutingData]): RDD[(String, String)] = {

    val result: RDD[(String, String)] =
      pffScoutingRDD.filter(pf => !pf.hangTime.equalsIgnoreCase(NOT_AVAILABLE))
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

  private def returnKickDirectionMissMatchExtractWithPlayId(pffScoutingRDD: RDD[PFFScoutingData]): RDD[(String, String)] = {

    val resutl = pffScoutingRDD
      .filter(t => !t.returnDirectionIntended.equalsIgnoreCase(NOT_AVAILABLE)
        && !t.returnDirectionActual.equalsIgnoreCase(NOT_AVAILABLE))
      .map(t => (t.playId, t.returnDirectionIntended, t.returnDirectionActual))
      .groupBy(t => t._1)
      .map {
        t =>
          (t._1,
            t._2
              .toList
              .filter(f => !f._2.equalsIgnoreCase(f._3))
              .map(f => (f._2, f._3))
              .flatMap {
                t => t._1 :: t._2 :: Nil
              }
              .mkString
              .groupBy(identity)
              .mapValues(_.length)
              .toList
              .map {
                str =>
                  val returnDirection = str._1
                  val count = str._2
                  s"$returnDirection$COLON_DELIMITER$count"
              }.mkString(PIPE_DELIMITER)
          )
      }
      .filter(t => t._2.nonEmpty)

    resutl.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def returnKickDirectionMissMatchExtractWithPlayIdToDf(pffScoutingRDD: RDD[PFFScoutingData], spark: SparkSession): DataFrame = {

    val kickDirectionMissMatch: RDD[(String, String)] =
      returnKickDirectionMissMatchExtractWithPlayId(pffScoutingRDD = pffScoutingRDD)

    spark
      .createDataFrame(kickDirectionMissMatch)
      .toDF("gameId", "ReturnKickDirections")
  }

}
