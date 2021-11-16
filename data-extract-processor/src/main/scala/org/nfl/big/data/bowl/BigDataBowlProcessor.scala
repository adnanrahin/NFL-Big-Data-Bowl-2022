package org.nfl.big.data.bowl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.nfl.big.data.bowl.constant.Constant
import org.nfl.big.data.bowl.dataextractors.{PFFScoutingDataExtractor, TrackingDataExtractor}
import org.nfl.big.data.bowl.dataloader._
import org.nfl.big.data.bowl.entity._

import java.lang.reflect.InvocationTargetException

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


    args(1) match {
      case "1" => {
        val touchdownDF = TrackingDataExtractor
          .findEventByEventNameToDF("touchdown", trackingRDD, spark)
        DataProcessorHelper.dataWriter(dataFrame = touchdownDF, dataPath = dataPath, directoryName = "touchdown")
      }
      case "2" => {
        val homeTouchDownLeftDF = TrackingDataExtractor
          .findHomeTeamEventToDF("touchdown", "left", trackingRDD, spark)
        DataProcessorHelper.dataWriter(dataFrame = homeTouchDownLeftDF, dataPath = dataPath, directoryName = "hometeamtouchdownleft")
      }
      case "3" => {
        val homeTouchDownRightDF = TrackingDataExtractor
          .findHomeTeamEventToDF("touchdown", "right", trackingRDD, spark)
        DataProcessorHelper.dataWriter(dataFrame = homeTouchDownRightDF, dataPath = dataPath, directoryName = "hometeamtouchdownright")
      }
      case "4" => {
        val awayTouchDownLeftDF = TrackingDataExtractor
          .findAwayTeamEventToDF("touchdown", "left", trackingRDD, spark)
        DataProcessorHelper.dataWriter(awayTouchDownLeftDF, dataPath, directoryName = "awayteamtouchdownleft")
      }
      case "5" => {
        val awayTouchDownRightDF = TrackingDataExtractor
          .findAwayTeamEventToDF("touchdown", "right", trackingRDD, spark)
        DataProcessorHelper.dataWriter(awayTouchDownRightDF, dataPath, directoryName = "awayteamtouchdownright")
      }
      case "6" => {
        val totalDistanceCoverInEachGame = TrackingDataExtractor
          .findTotalDistanceRunInEachGameToDf(trackingRDD = trackingRDD, spark = spark)
        DataProcessorHelper.dataWriter(totalDistanceCoverInEachGame, dataPath, directoryName = "totaldistance")
      }
      case "7" => {
        val totalHangingTimeInEachGame = PFFScoutingDataExtractor
          .findTotalDistanceRunInEachGameToDf(pffScoutingRDD = pffScoutingRDD, spark = spark)
        DataProcessorHelper.dataWriter(totalHangingTimeInEachGame, dataPath, directoryName = "totalhangingtime")
      }
      case "8" => {
        val puntRushers = PFFScoutingDataExtractor
          .puntRushersToDF(pffScoutingRDD = pffScoutingRDD, spark = spark)
        DataProcessorHelper.dataWriter(puntRushers, dataPath, directoryName = "extractpuntrushers")
      }
      case "9" => {
        val kickDirectionMissMatch = PFFScoutingDataExtractor
          .kickDirectionMissMatchExtractToDf(pffScoutingRDD = pffScoutingRDD, spark = spark)
        DataProcessorHelper.dataWriter(kickDirectionMissMatch, dataPath, directoryName = "kickdirectionmissmatch")
      }case "10" => {
        val returnKickDirectionMissMatch = PFFScoutingDataExtractor
          .returnKickDirectionMissMatchExtractWithPlayIdToDf(pffScoutingRDD = pffScoutingRDD, spark = spark)
        DataProcessorHelper.dataWriter(returnKickDirectionMissMatch, dataPath, directoryName = "returnkickdirectionmissmatch")
      }
      case _ => {
        try {

        } catch {
          case exception: ArrayIndexOutOfBoundsException => println("Array index out of bound, args(1) is missing from programs argument " + exception)
          case exception: InvocationTargetException => println("Missing parameters from run arguments " + exception)
        } finally {
          spark.close()
        }
      }
    }

    spark.close()

  }

}
