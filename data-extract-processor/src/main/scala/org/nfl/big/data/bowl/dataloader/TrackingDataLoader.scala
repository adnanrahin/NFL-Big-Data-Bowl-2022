package org.nfl.big.data.bowl.dataloader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.nfl.big.data.bowl.entity.Tracking

class TrackingDataLoader(filePath: String, spark: SparkSession) extends DataLoader {

  override def loadRDD(): RDD[Tracking] = {

    val trackingDataCSV: RDD[String] = this.spark.sparkContext.textFile(this.filePath)

    val trackingDataRDD: RDD[Tracking] = trackingDataCSV
      .map(row => row.split(",", -1))
      .map(str =>
        Tracking(
          str(0).replace("\"", ""),
          str(1).replace("\"", ""),
          str(2).replace("\"", ""),
          str(3).replace("\"", ""),
          str(4).replace("\"", ""),
          str(5).replace("\"", ""),
          str(6).replace("\"", ""),
          str(7).replace("\"", ""),
          str(8).replace("\"", ""),
          str(9).replace("\"", ""),
          str(10).replace("\"", ""),
          str(11).replace("\"", ""),
          str(12).replace("\"", ""),
          str(13).replace("\"", ""),
          str(14).replace("\"", ""),
          str(15).replace("\"", ""),
          str(16).replace("\"", ""),
          str(17).replace("\"", "")
        )
      )
      .mapPartitionsWithIndex {
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      }

    trackingDataRDD.persist(StorageLevel.MEMORY_AND_DISK)

  }

}
