package org.nfl.big.data.bowl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.nfl.big.data.bowl.constant.Constant
import org.nfl.big.data.bowl.dataloader._
import org.nfl.big.data.bowl.entity._

object BigDataBowlProcessor {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("BigDataBowlProcessor")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath = args(0)

    val gameDataLoader: GameDataLoader = new GameDataLoader(dataPath + Constant.GAMES, spark)
/*
    val pffScoutingDataLoader: PFFScoutingDataLoader = new PFFScoutingDataLoader(dataPath + Constant.PFFSCOUNTINGDATA, spark)
    val playersDataLoader: PlayersDataLoader = new PlayersDataLoader(dataPath + Constant.PLAYERS, spark)
    val playsDataLoader: PlaysDataLoader = new PlaysDataLoader(dataPath + Constant.PLAYS, spark)
    val trackingDataLoader: TrackingDataLoader = new TrackingDataLoader(dataPath + Constant.TRACKING, spark)
*/

    val gameRDD: RDD[Games] = gameDataLoader.loadRDD()
/*    val pffScoutingRDD: RDD[PFFScoutingData] = pffScoutingDataLoader.loadRDD()
    val playersRDD: RDD[Players] = playersDataLoader.loadRDD()
    val playsRDD: RDD[Plays] = playsDataLoader.loadRDD()
    val trackingRDD: RDD[Tracking] = trackingDataLoader.loadRDD()*/

    gameRDD.foreach(trackt => println(trackt))

    //findEventByEventNameToDF("none", trackingRDD, spark)

  }

  private def findEventByEventName(event: String, trackingRDD: RDD[Tracking]): RDD[Tracking] = {

    val events: RDD[Tracking] =
      trackingRDD.filter(track => track.event.toString.equals(event))

    events/*.persist(StorageLevel.MEMORY_AND_DISK)*/
  }

  def findEventByEventNameToDF(event: String, trackingRDD: RDD[Tracking], spark: SparkSession): Unit = {

    val events: RDD[Tracking] = findEventByEventName(event, trackingRDD)

    spark
      .createDataFrame(events)
      .show(5, truncate = false)

  }

}
