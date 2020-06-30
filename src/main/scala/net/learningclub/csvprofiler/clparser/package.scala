package net.learningclub.csvprofiler

import org.rogach.scallop.{ScallopConf, ValueConverter}

package object clparser {

  case class CsvColumnSchema(
                                       existingColName: String,
                                       newColumnName: String,
                                       newDataType: String,
                                       dateExpression: Option[String] = None
                                     )

  case class ValidationError(errorMsg: String)

  class InputArguments(arguments: Seq[String])
                      (implicit conv: ValueConverter[List[CsvColumnSchema]]) extends ScallopConf(arguments) {
    val schema = opt[List[CsvColumnSchema]](required = true, name = "column")
    val delimiter = opt[String](required = false, name = "delimiter")
    val sourceDir = opt[String](required = true, name = "dir")
    val sourceFileName = opt[String](required = true, name = "file")
    verify()
  }

}
