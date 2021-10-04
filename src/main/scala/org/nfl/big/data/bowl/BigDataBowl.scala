package org.nfl.big.data.bowl

import org.apache.spark.sql.SparkSession

object BigDataBowl {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("BigDataBowl")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

  }

}
