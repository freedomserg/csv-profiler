package net.learningclub.profiler

import net.learningclub.TestSparkSession
import net.learningclub.csvprofiler.Profiler
import net.learningclub.csvprofiler.clparser.{InputArgsParser, InputArguments}
import org.scalatest._
import org.scalatest.funsuite._
import org.scalatest.matchers._

class ProfilerSuite extends AnyFunSuite with should.Matchers with TestSparkSession {

  implicit val argsParser = InputArgsParser()
  val clInput = Seq(
    "--dir", "src/test/resources/",
    "--file", "sample.csv",
    "--column", "name", "name", "string",
    "--column", "age", "age", "integer",
    "--column", "gender", "gender", "string",
    "--column", "birthday", "birthday", "date", "dd-MM-yyyy",
    "--delimiter", ",")
  //  val inputArguments = new InputArguments(clInput)

  test("Profiling single-quoted file with comma delimiter") {
    // GIVEN
    val inputArguments = new InputArguments(clInput)

    // WHEN
    val profiles = Profiler.run(spark, inputArguments)

    //THEN
    assert(
      profiles.exists(p => {
        p.column == "name" &&
          p.unique_values == 2 &&
          p.values("John") == 1 &&
          p.values("Lisa") == 1
      })
    )

    assert(
      profiles.exists(p => {
        p.column == "age" &&
          p.unique_values == 1 &&
          p.values("26") == 2
      })
    )

    assert(
      profiles.exists(p => {
        p.column == "birthday" &&
          p.unique_values == 1 &&
          p.values("1995-01-26") == 3
      })
    )

    assert(
      profiles.exists(p => {
        p.column == "gender" &&
          p.unique_values == 2 &&
          p.values("female") == 1 &&
          p.values("male") == 2
      })
    )
  }

  test("Profiling double-quoted file with comma delimiter") {
    // GIVEN
    val cl = Seq(
      "--dir", "src/test/resources/",
      "--file", "sample_double_quoted.csv",
      "--column", "name", "name", "string",
      "--column", "age", "age", "integer")
    val inputArguments = new InputArguments(cl)

    // WHEN
    val profiles = Profiler.run(spark, inputArguments)

    //THEN
    assert(
      profiles.exists(p => {
        p.column == "name" &&
          p.unique_values == 2 &&
          p.values("John") == 1 &&
          p.values("Lisa") == 1
      })
    )

    assert(
      profiles.exists(p => {
        p.column == "age" &&
          p.unique_values == 1 &&
          p.values("26") == 1
      })
    )
  }

  test("Profiling double-quoted file with bar delimiter") {
    // GIVEN
    val cl = Seq(
      "--dir", "src/test/resources/",
      "--file", "sample_bar_delimiter.csv",
      "--column", "name", "name", "string",
      "--column", "age", "age", "integer",
      "--delimiter", "|")
    val inputArguments = new InputArguments(cl)

    // WHEN
    val profiles = Profiler.run(spark, inputArguments)

    //THEN
    assert(
      profiles.exists(p => {
        p.column == "name" &&
          p.unique_values == 2 &&
          p.values("John") == 1 &&
          p.values("Lisa") == 1
      })
    )

    assert(
      profiles.exists(p => {
        p.column == "age" &&
          p.unique_values == 1 &&
          p.values("26") == 1
      })
    )
  }

  test("Profiling non-quoted file with quoted empty values") {
    // GIVEN
    val cl = Seq(
      "--dir", "src/test/resources/",
      "--file", "sample_no_quotes.csv",
      "--column", "name", "name", "string",
      "--column", "age", "age", "integer")
    val inputArguments = new InputArguments(cl)

    // WHEN
    val profiles = Profiler.run(spark, inputArguments)

    //THEN
    assert(
      profiles.exists(p => {
        p.column == "name" &&
          p.unique_values == 2 &&
          p.values("John") == 1 &&
          p.values("Lisa") == 1
      })
    )

    assert(
      profiles.exists(p => {
        p.column == "age" &&
          p.unique_values == 1 &&
          p.values("26") == 2
      })
    )
  }

}
