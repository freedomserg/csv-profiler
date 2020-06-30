package net.learningclub

import org.apache.spark.sql.SparkSession

trait TestSparkSession {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .getOrCreate()

}
