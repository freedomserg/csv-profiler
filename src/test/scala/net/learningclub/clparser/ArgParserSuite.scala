package net.learningclub.clparser

import net.learningclub.csvprofiler.clparser.{InputArgsParser, InputArguments}
import org.scalatest._
import org.scalatest.funsuite._
import org.scalatest.matchers._

class ArgParserSuite extends AnyFunSuite with should.Matchers {
  implicit val argsParser = InputArgsParser()

  test("All opts are present - happy path") {
    // GIVEN
    val args = Seq(
      "--dir", "src/main/resources/",
      "--file", "sample.csv",
      "--column", "name", "name", "string",
      "--column", "age", "age", "integer",
      "--column", "gender", "gender", "string",
      "--column", "birthday", "birthday", "date", "dd-MM-yyyy",
      "--delimiter", ",")

    // WHEN
    val inputArguments = new InputArguments(args)

    // THEN
    val sourceDir = inputArguments.sourceDir()
    val sourceFile = inputArguments.sourceFileName()
    val delimiterOpt = inputArguments.delimiter.toOption
    sourceDir shouldEqual "src/main/resources/"
    sourceFile shouldEqual "sample.csv"
    delimiterOpt.isDefined shouldEqual true
    delimiterOpt.get shouldEqual ","

    val schema = inputArguments.schema()
    assert(
      schema.exists(c => {
        c.existingColName == "name" &&
          c.newColumnName == "name" &&
          c.newDataType == "string"
      })
    )

    assert(
      schema.exists(c => {
        c.existingColName == "age" &&
          c.newColumnName == "age" &&
          c.newDataType == "integer"
      })
    )

    assert(
      schema.exists(c => {
        c.existingColName == "gender" &&
          c.newColumnName == "gender" &&
          c.newDataType == "string"
      })
    )

    assert(
      schema.exists(c => {
        c.existingColName == "birthday" &&
          c.newColumnName == "birthday" &&
          c.newDataType == "date" &&
          c.dateExpression.isDefined &&
          c.dateExpression.get == "dd-MM-yyyy"
      })
    )
  }
}
