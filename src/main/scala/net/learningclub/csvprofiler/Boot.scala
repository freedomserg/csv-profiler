package net.learningclub.csvprofiler

import io.circe.syntax._
import net.learningclub.csvprofiler.Utils.writeOutputToFile
import net.learningclub.csvprofiler.clparser.{InputArgsParser, InputArguments}
import org.apache.spark.sql.SparkSession

object Boot {

  def main(args: Array[String]): Unit = {

    implicit val argsParser = InputArgsParser()
    val inputArguments = new InputArguments(args)

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("CSV Profiler")
      .config("spark.driver.memory", "2g")
      .getOrCreate()


    val fieldsProfiles = Profiler.run(spark, inputArguments)
    val resultStr = fieldsProfiles.asJson.spaces2

    writeOutputToFile(inputArguments, resultStr)
    println(resultStr)
  }

}
