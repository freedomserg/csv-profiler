package net.learningclub.csvprofiler.clparser

import java.text.SimpleDateFormat

import net.learningclub.csvprofiler.SupportedDataTypes
import org.rogach.scallop.{ArgType, ValueConverter}

import scala.util.{Failure, Success, Try}

object InputArgsParser {

  def apply(): ValueConverter[List[CsvColumnSchema]] = new ValueConverter[List[CsvColumnSchema]] {
    private val csvColumnOpt = "column"

    override def parse(s: List[(String, List[String])]): Either[String, Option[List[CsvColumnSchema]]] = {
      val parseResult = s.foldLeft((List[ValidationError](), List[CsvColumnSchema]())) { case (accum, current) =>
        val currentOpt = current._1
        val currentArgs = current._2
        val errorsAccum = accum._1
        val parsedArgsAccum = accum._2
        if (errorsAccum.nonEmpty) accum
        else {
          currentOpt match {
            case o if o == csvColumnOpt =>
              if (currentArgs.size < 3) {
                val newErrorsAccum = ValidationError("Values provided not for all required fields.") :: errorsAccum
                newErrorsAccum -> parsedArgsAccum
              } else if (SupportedDataTypes.DATE.toString == currentArgs(2) && currentArgs.size < 4 ) {
                val newErrorsAccum = ValidationError("Please provide a value for Date expression argument") :: errorsAccum
                newErrorsAccum -> parsedArgsAccum
              } else {
                val newDataTypeValue = currentArgs(2)
                newDataTypeValue match {
                  case v if v == SupportedDataTypes.STRING.toString ||
                    v == SupportedDataTypes.INTEGER.toString ||
                    v == SupportedDataTypes.BOOLEAN.toString =>
                    val newArgs = CsvColumnSchema(
                      existingColName = currentArgs.head,
                      newColumnName = currentArgs(1),
                      newDataType = v
                    )
                    errorsAccum -> (newArgs :: parsedArgsAccum)

                  case v if v == SupportedDataTypes.DATE.toString =>
                    val dateExpression = currentArgs(3)
                    Try(new SimpleDateFormat(dateExpression)) match {
                      case Success(_) =>
                        val newArgs = CsvColumnSchema(
                          existingColName = currentArgs.head,
                          newColumnName = currentArgs(1),
                          newDataType = v,
                          dateExpression = Some(dateExpression)
                        )
                        errorsAccum -> (newArgs :: parsedArgsAccum)
                      case Failure(_) =>
                        val newErrorsAccum = ValidationError("Provided date expression is not valid") :: errorsAccum
                        newErrorsAccum -> parsedArgsAccum
                    }
                  case _ =>
                    val newErrorsAccum = ValidationError(s"Data type '$newDataTypeValue' is not supported") :: errorsAccum
                    newErrorsAccum -> parsedArgsAccum
                }
              }
            case _ => accum
          }
        }

      }

      val parseErrors = parseResult._1
      if (parseErrors.nonEmpty) {
        Left(parseErrors.head.errorMsg)
      } else {
        val parsedArgs = parseResult._2
        Right(Some(parsedArgs))
      }

    }

    val manifest = implicitly[Manifest[CsvColumnSchema]]
    override val argType: ArgType.V = org.rogach.scallop.ArgType.LIST
  }

}
