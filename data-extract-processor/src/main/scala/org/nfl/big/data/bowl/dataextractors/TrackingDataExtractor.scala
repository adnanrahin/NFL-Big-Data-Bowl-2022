package org.nfl.big.data.bowl.dataextractors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.entity.Tracking

object TrackingDataExtractor {

  final val HOME: String = "home"

  private def findEventByEventName(event: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val events: RDD[Tracking] =
      trackingRDD
        .filter(track => track.event.equalsIgnoreCase(event))

    events.persist(StorageLevel.MEMORY_AND_DISK)
  }

  private def findHomeTeamEvent(event: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val result: RDD[Tracking] =
      trackingRDD.filter {
        t => t.event.equalsIgnoreCase(event) && t.team.equalsIgnoreCase(HOME)
      }

    result.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def findEventByEventNameToDF(event: String, trackingRDD: RDD[Tracking], spark: SparkSession): DataFrame = {
    val events: RDD[Tracking] = findEventByEventName(event, trackingRDD)
    spark.createDataFrame(events)
  }

}
