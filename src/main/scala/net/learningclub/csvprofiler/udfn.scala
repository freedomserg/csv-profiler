package net.learningclub.csvprofiler

import java.sql.Date
import java.text.SimpleDateFormat

import net.learningclub.csvprofiler.Utils.trimSpecialChars
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.{Failure, Success, Try}

object udfn {

  val toInt: UserDefinedFunction = udf[Integer, String]((value: String) => {
    if (value == null) null
    else {
      val trimmed = trimSpecialChars(value).trim
      Try(trimmed.toInt)
        .map(Integer.valueOf).getOrElse(null)
    }
  })


  val toStr: UserDefinedFunction = udf[String, String]((value: String) => {
    if (value == null) null
    else trimSpecialChars(value).trim
  }
  )

  val toBool: UserDefinedFunction = udf[java.lang.Boolean, String]((value: String) => {
    if (value == null) null
    else {
      val trimmed = trimSpecialChars(value).trim
      Try(trimmed.toBoolean)
        .map(java.lang.Boolean.valueOf).getOrElse(null)
    }
  })

  def toDate(format: String): UserDefinedFunction = udf[Date, String]((value: String) => {
    if (value == null || format == null) null
    else {
      val dateFormat = new SimpleDateFormat(format)
      dateFormat.setLenient(true)
      val trimmed = trimSpecialChars(value).trim
      Try(dateFormat.parse(trimmed)) match {
        case Success(d) => new Date(d.getTime)
        case Failure(_) => null
      }
    }
  })

}
