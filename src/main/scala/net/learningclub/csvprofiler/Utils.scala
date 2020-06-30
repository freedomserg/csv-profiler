package net.learningclub.csvprofiler

import java.io.{File, PrintWriter}

import net.learningclub.csvprofiler.clparser.{CsvColumnSchema, InputArguments}
import net.learningclub.csvprofiler.udfn.{toBool, toDate, toInt, toStr}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

import scala.util.matching.Regex

object Utils {

  val DROP_SURROUNDING_SPECIAL_CHARS_PATTERN: Regex = "(^[^a-zA-Z0-9]*)(.*?)([^a-zA-Z0-9]*$)".r

  def trimSpecialChars(source: String): String =
    DROP_SURROUNDING_SPECIAL_CHARS_PATTERN.replaceAllIn(source, m => m.group(2))

  def applyNewSchema(col: Column, schema: CsvColumnSchema): Column = {
    val dataType = schema.newDataType
    SupportedDataTypes.withName(dataType) match {
      case SupportedDataTypes.STRING => toStr(col)
      case SupportedDataTypes.INTEGER => toInt(col)
      case SupportedDataTypes.BOOLEAN => toBool(col)
      case SupportedDataTypes.DATE =>
        val dateFormat = schema.dateExpression.orNull // Date expression param was validated by net.learningclub.csvprofiler.clparser.InputArgsParser
        toDate(dateFormat)(col)
    }
  }

  def countUniqueValues(col: Column, colName: String, resultDfSchema: StructType): Column = {
    resultDfSchema(colName).dataType match {
      case ArrayType(StringType, _) => countUnique(col)
      case ArrayType(IntegerType, _) => countUnique(col.cast(ArrayType(StringType)))
      case ArrayType(BooleanType, _) => countUnique(col.cast(ArrayType(StringType)))
      case ArrayType(DateType, _) => countUnique(col.cast(ArrayType(StringType)))
    }

  }

  val countUnique: UserDefinedFunction = udf[(String, Int), Seq[String]]((values: Seq[String]) => values.head -> values.size)

  def writeOutputToFile(inputArgs: InputArguments, text: String): Unit = {
    val sourceDirStr = inputArgs.sourceDir()
    val sourceFileName = inputArgs.sourceFileName()
    val outputFullPath = sourceDirStr + sourceFileName.replace(".csv", "_profile")

    val writer = new PrintWriter(new File(outputFullPath))
    writer.write(text)
    writer.close()
  }

}
