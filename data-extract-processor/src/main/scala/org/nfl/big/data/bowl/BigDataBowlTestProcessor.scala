package org.nfl.big.data.bowl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.nfl.big.data.bowl.constant.Constant
import org.nfl.big.data.bowl.dataextractors.PFFScoutingDataExtractor
import org.nfl.big.data.bowl.dataloader.PFFScoutingDataLoader
import org.nfl.big.data.bowl.entity.PFFScoutingData

object BigDataBowlTestProcessor {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("BigDataBowlProcessor")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val dataPath = args(0)

    val pffScoutingDataLoader: PFFScoutingDataLoader = new PFFScoutingDataLoader(dataPath + Constant.PFFSCOUNTINGDATA, spark)

    val pffScoutingRDD: RDD[PFFScoutingData] = pffScoutingDataLoader.loadRDD()

    val pfRDD = PFFScoutingDataExtractor.findTotalDistanceRunInEachGameToDf(pffScoutingRDD, spark = spark)



  }

}
