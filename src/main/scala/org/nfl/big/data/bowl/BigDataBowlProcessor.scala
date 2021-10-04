package org.nfl.big.data.bowl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.nfl.big.data.bowl.dataloader.GameDataLoader
import org.nfl.big.data.bowl.entity.Games

object BigDataBowlProcessor {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("BigDataBowl")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val gameDataLoader: GameDataLoader = new GameDataLoader("dataset/games.csv", spark)

    val gameRDD: RDD[Games] = gameDataLoader.loadRDD()

    gameRDD.foreach(row => println(row.gameId))

  }

}
