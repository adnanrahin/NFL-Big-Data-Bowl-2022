#!/bin/bash

SPARK_HOME=$SPARK_HOME
APP_JAR="/home/rahin/source-code/spark/NFL-Big-Data-Bowl-2022/data-extract-processor/target/data-extract-processor-1.0-SNAPSHOT.jar"
INPUT_PATH="/sandbox/storage/data/nfl-super-bowl-data-2020/input_data/"
OUTPUT_PATH="/sandbox/storage/data/nfl-super-bowl-data-2020/filter_data/"
PARTITIONS="2"

$SPARK_HOME/bin/spark-submit \
    --master spark://dev-server01:7077 \
    --deploy-mode cluster \
    --class org.nfl.big.data.bowl.BigDataBowlProcessor \
    --name BigDataBowlProcessorSpark \
    --driver-memory 2G \
    --driver-cores 4 \
    --executor-memory 2G \
    --executor-cores 2 \
    --total-executor-cores 12 \
    $APP_JAR $INPUT_PATH $OUTPUT_PATH $PARTITIONS
