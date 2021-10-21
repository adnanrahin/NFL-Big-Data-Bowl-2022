package org.nfl.big.data.bowl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.nfl.big.data.bowl.constant.Constant
import org.nfl.big.data.bowl.dataextractors.TrackingDataExtractor
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
    val pffScoutingDataLoader: PFFScoutingDataLoader = new PFFScoutingDataLoader(dataPath + Constant.PFFSCOUNTINGDATA, spark)
    val playersDataLoader: PlayersDataLoader = new PlayersDataLoader(dataPath + Constant.PLAYERS, spark)
    val playsDataLoader: PlaysDataLoader = new PlaysDataLoader(dataPath + Constant.PLAYS, spark)
    val trackingDataLoader: TrackingDataLoader = new TrackingDataLoader(dataPath + Constant.TRACKING, spark)

    val gameRDD: RDD[Games] = gameDataLoader.loadRDD()
    val pffScoutingRDD: RDD[PFFScoutingData] = pffScoutingDataLoader.loadRDD()
    val playersRDD: RDD[Players] = playersDataLoader.loadRDD()
    val playsRDD: RDD[Plays] = playsDataLoader.loadRDD()
    val trackingRDD: RDD[Tracking] = trackingDataLoader.loadRDD()

    val touchdownDF = TrackingDataExtractor
      .findEventByEventNameToDF("touchdown", trackingRDD, spark)
    dataWriter(dataFrame = touchdownDF, dataPath = dataPath, directoryName = "touchdown")

    val homeTouchDownLeftDF = TrackingDataExtractor
      .findHomeTeamEventToDF("touchdown", "left", trackingRDD, spark)
    dataWriter(dataFrame = homeTouchDownLeftDF, dataPath = dataPath, directoryName = "hometeamtouchdownleft")

    val homeTouchDownRightDF = TrackingDataExtractor
      .findHomeTeamEventToDF("touchdown", "right", trackingRDD, spark)
    dataWriter(dataFrame = homeTouchDownRightDF, dataPath = dataPath, directoryName = "hometeamtouchdownright")

    val awayTouchDownLeftDF = TrackingDataExtractor
      .findAwayTeamEventToDF("touchdown", "left", trackingRDD, spark)
    dataWriter(awayTouchDownLeftDF, dataPath, directoryName = "awayteamtouchdownleft")

    val awayTouchDownRightDF = TrackingDataExtractor
      .findAwayTeamEventToDF("touchdown", "right", trackingRDD, spark)
    dataWriter(awayTouchDownRightDF, dataPath, directoryName = "awayteamtouchdownright")



    spark.close()

  }

  def dataWriter(dataFrame: DataFrame, dataPath: String, directoryName: String): Unit = {

    val destinationDirectory: String = dataPath + Constant.FILTER_DIR + "/" + directoryName

    dataFrame
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationDirectory)
  }

}
