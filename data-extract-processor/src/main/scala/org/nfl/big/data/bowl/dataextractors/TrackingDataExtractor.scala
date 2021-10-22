package org.nfl.big.data.bowl.dataextractors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.entity.Tracking

import scala.util.Try

object TrackingDataExtractor {

  final val HOME: String = "home"
  final val AWAY: String = "away"

  private def findEventByEventName(event: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val events: RDD[Tracking] =
      trackingRDD
        .filter(track => track.event.equalsIgnoreCase(event))

    events.persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def findHomeTeamEvent(event: String, playDirection: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val result: RDD[Tracking] =
      trackingRDD.filter(
        t => {
          t.event.equalsIgnoreCase(event) &&
            t.team.equalsIgnoreCase(HOME) &&
            t.playDirection.equalsIgnoreCase(playDirection)
        }
      )

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def findAwayTeamEvent(event: String, playDirection: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val result: RDD[Tracking] =
      trackingRDD.filter(
        t => {
          t.event.equalsIgnoreCase(event) &&
            t.team.equalsIgnoreCase(HOME) &&
            t.playDirection.equalsIgnoreCase(playDirection)
        }
      )

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def findTotalDistanceRunInEachGame(trackingRDD: RDD[Tracking]): RDD[(String, String)] = {

    val result: RDD[(String, String)] = trackingRDD
      .filter(track => isNumeric(track.dis))
      .map {
        track => (track.gameId, track.dis.toDouble)
      }
      .groupBy(_._1)
      .map {
        track =>
          (track._1,
            BigDecimal
              .apply(track._2.foldLeft(0.0)(_ + _._2))
              .setScale(6, BigDecimal.RoundingMode.HALF_EVEN)
              .toDouble)
      }
      .sortBy(-_._2)
      .map {
        kv => (kv._1, kv._2.toString)
      }

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def findHomeTeamEventToDF(event: String, playDirection: String, trackingRDD: RDD[Tracking], spark: SparkSession): DataFrame = {

    val eventRDD = findHomeTeamEvent(event, playDirection, trackingRDD)

    spark
      .createDataFrame(eventRDD)
  }

  def findAwayTeamEventToDF(event: String, playDirection: String, trackingRDD: RDD[Tracking], spark: SparkSession): DataFrame = {

    val eventRDD = findAwayTeamEvent(event, playDirection, trackingRDD)

    spark
      .createDataFrame(eventRDD)
  }

  def findEventByEventNameToDF(event: String, trackingRDD: RDD[Tracking], spark: SparkSession): DataFrame = {

    val events: RDD[Tracking] = findEventByEventName(event, trackingRDD)

    spark
      .createDataFrame(events)
  }

  def findTotalDistanceRunInEachGameToDf(trackingRDD: RDD[Tracking], spark: SparkSession): DataFrame = {

    val distanceRDD: RDD[(String, String)] = findTotalDistanceRunInEachGame(trackingRDD = trackingRDD)

    spark
      .createDataFrame(distanceRDD)
      .toDF("gameId", "totalDistance")
  }

  def isNumeric(num: String): Boolean = {

    def isShort(aString: String): Boolean = Try(aString.toLong).isSuccess

    def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess

    def isLong(aString: String): Boolean = Try(aString.toLong).isSuccess

    def isDouble(aString: String): Boolean = Try(aString.toDouble).isSuccess

    def isFloat(aString: String): Boolean = Try(aString.toFloat).isSuccess

    if (isShort(num) || isInt(num) || isLong(num) || isDouble(num) || isFloat(num)) true
    else false
  }

}
