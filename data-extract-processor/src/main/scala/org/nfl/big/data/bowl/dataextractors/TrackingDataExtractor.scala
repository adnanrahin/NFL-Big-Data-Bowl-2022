package org.nfl.big.data.bowl.dataextractors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.entity.Tracking

object TrackingDataExtractor {

  private def findEventByEventName(event: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val events: RDD[Tracking] =
      trackingRDD
        .filter(track => track.event.equalsIgnoreCase(event))

    events.persist(StorageLevel.MEMORY_AND_DISK)
  }

  def findEventByEventNameToDF(event: String, trackingRDD: RDD[Tracking], spark: SparkSession, dataPath: String): Unit = {

    val events: RDD[Tracking] = findEventByEventName(event, trackingRDD)

    val df = spark.createDataFrame(events)

    df.write.parquet(dataPath + "filterdata" + event)

  }

}
