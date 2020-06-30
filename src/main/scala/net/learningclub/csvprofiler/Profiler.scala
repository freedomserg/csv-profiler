package net.learningclub.csvprofiler

import net.learningclub.csvprofiler.Utils._
import net.learningclub.csvprofiler.clparser.{CsvColumnSchema, InputArguments}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Profiler {

  def run(spark: SparkSession, inputArguments: InputArguments): List[FieldProfile] = {

    val sourceDirStr = inputArguments.sourceDir()
    val sourceFileName = inputArguments.sourceFileName()
    val sourceFullPath = sourceDirStr + sourceFileName
    val schemaRequest = inputArguments.schema()
    val delimiter = inputArguments.delimiter.toOption.getOrElse(",")

    val schemaRequestByColName: Map[String, CsvColumnSchema] = schemaRequest.groupBy(_.existingColName).mapValues(_.head)

    val inputDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("sep", delimiter)
      .load(sourceFullPath)

    val initialDfSchema = inputDF.schema

    val fieldNamesNoSpecialChars = initialDfSchema.fields.foldLeft(Array[StructField]()) { case (accum, field) =>
      val fieldNoSpecialChars = field.copy(name = trimSpecialChars(field.name).trim)
      accum :+ fieldNoSpecialChars
    }
    val schemaNoSpecialChars = new StructType(fieldNamesNoSpecialChars)
    val stringTypeFields = schemaNoSpecialChars.fields.filter(_.dataType == StringType).map(_.name)

    val existingFieldNames = schemaNoSpecialChars.fields.map(_.name)
    val requestedFieldNames = schemaRequestByColName.keys.toSeq
    val colToDrop = existingFieldNames.diff(requestedFieldNames)

    val inputDfSchemaNoSpecialChars = spark.createDataFrame(inputDF.rdd, schemaNoSpecialChars)
    val resultDF = inputDfSchemaNoSpecialChars
      .filter(r => {
        stringTypeFields.foldLeft(true) { case (accum, nextFieldName) =>
          if (!accum) accum else {
            val fieldValue = r.getAs[String](nextFieldName)
            fieldValue == null || trimSpecialChars(fieldValue).trim.nonEmpty
          }
        }
      })
      .drop(colToDrop: _*)
      .transform(df => {
        schemaRequestByColName.foldLeft(df) { case (accumDf, currCol) =>
          val colName = currCol._1
          val newColSchema = currCol._2
          val newColName = newColSchema.newColumnName
          val tempColName = "_" + newColName
          val existingCol = accumDf(colName)
          accumDf
            .withColumn(tempColName, applyNewSchema(existingCol, newColSchema))
            .drop(colName)
            .withColumnRenamed(tempColName, newColName)
        }
      }).cache()

    val resultDfFieldNames = resultDF.schema.fieldNames

    resultDfFieldNames.foldLeft(List[FieldProfile]()) { case (accum, currentField) =>
      val valuesTempCol = "values"
      val valuesCountedTempCol = "values_counted"

      val groupedByCurrentFieldDf = resultDF
        .groupBy(currentField)
        .agg(
          collect_list(currentField).alias(valuesTempCol)
        )
        .coalesce(4)

      val groupedDfSchema = groupedByCurrentFieldDf.schema

      val resultRowsArray: Array[Row] = groupedByCurrentFieldDf
        .withColumn(valuesCountedTempCol, countUniqueValues(col(valuesTempCol), valuesTempCol, groupedDfSchema))
        .select(col(valuesCountedTempCol))
        .where(col(currentField).isNotNull)
        .collect()

      val valuesCountMap: Map[String, Int] = resultRowsArray.foldLeft(Map[String, Int]()) { case (accum, currRow) =>
        val struct = currRow.getStruct(0)
        val value = struct.getString(0)
        val valueCount = struct.getInt(1)
        accum + (value -> valueCount)
      }

      val uniqueValuesCount = resultRowsArray.length
      val output = FieldProfile(
        column = currentField,
        unique_values = uniqueValuesCount,
        values = valuesCountMap
      )
      output :: accum
    }

  }

}
