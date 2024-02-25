package org.nfl.big.data.bowl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.nfl.big.data.bowl.constant.Constant
import org.nfl.big.data.bowl.dataextractors.{PFFScoutingDataExtractor, TrackingDataExtractor}
import org.nfl.big.data.bowl.dataloader._
import org.nfl.big.data.bowl.entity._

object BigDataBowlProcessor {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("BigDataBowlProcessor")
      //.master("local[*]")
      .getOrCreate()

    val dataPath = args(0)
    val outputPath = args(1)

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
    DataProcessorHelper.dataWriter(dataFrame = touchdownDF, dataPath = outputPath, directoryName = "touchdown")

    val homeTouchDownLeftDF = TrackingDataExtractor
      .findHomeTeamEventToDF("touchdown", "left", trackingRDD, spark)
    DataProcessorHelper.dataWriter(dataFrame = homeTouchDownLeftDF, dataPath = outputPath, directoryName = "hometeamtouchdownleft")

    val homeTouchDownRightDF = TrackingDataExtractor
      .findHomeTeamEventToDF("touchdown", "right", trackingRDD, spark)
    DataProcessorHelper.dataWriter(dataFrame = homeTouchDownRightDF, dataPath = outputPath, directoryName = "hometeamtouchdownright")

    val awayTouchDownLeftDF = TrackingDataExtractor
      .findAwayTeamEventToDF("touchdown", "left", trackingRDD, spark)
    DataProcessorHelper.dataWriter(awayTouchDownLeftDF, outputPath, directoryName = "awayteamtouchdownleft")

    val awayTouchDownRightDF = TrackingDataExtractor
      .findAwayTeamEventToDF("touchdown", "right", trackingRDD, spark)
    DataProcessorHelper.dataWriter(awayTouchDownRightDF, outputPath, directoryName = "awayteamtouchdownright")

    val totalDistanceCoverInEachGame = TrackingDataExtractor
      .findTotalDistanceRunInEachGameToDf(trackingRDD = trackingRDD, spark = spark)
    DataProcessorHelper.dataWriter(totalDistanceCoverInEachGame, outputPath, directoryName = "totaldistance")

    val totalHangingTimeInEachGame = PFFScoutingDataExtractor
      .findTotalDistanceRunInEachGameToDf(pffScoutingRDD = pffScoutingRDD, spark = spark)
    DataProcessorHelper.dataWriter(totalHangingTimeInEachGame, outputPath, directoryName = "totalhangingtime")

    val puntRushers = PFFScoutingDataExtractor
      .puntRushersToDF(pffScoutingRDD = pffScoutingRDD, spark = spark)
    DataProcessorHelper.dataWriter(puntRushers, outputPath, directoryName = "extractpuntrushers")

    val kickDirectionMissMatch = PFFScoutingDataExtractor
      .kickDirectionMissMatchExtractToDf(pffScoutingRDD = pffScoutingRDD, spark = spark)
    DataProcessorHelper.dataWriter(kickDirectionMissMatch, outputPath, directoryName = "kickdirectionmissmatch")

    val returnKickDirectionMissMatch = PFFScoutingDataExtractor
      .returnKickDirectionMissMatchExtractWithPlayIdToDf(pffScoutingRDD = pffScoutingRDD, spark = spark)
    DataProcessorHelper.dataWriter(returnKickDirectionMissMatch, outputPath, directoryName = "returnkickdirectionmissmatch")

  }
}
